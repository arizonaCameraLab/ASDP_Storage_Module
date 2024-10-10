/*
 * Copyright (C) 2024: Arizona Board of Regents on Behalf of the University of Arizona
 */

#include "Storage_Module.h"
#include "SpinFreePacketTimer.h"
#include <iostream>
#include <algorithm>
#include <limits>
#include <filesystem>
#include <thread>
#include <atomic>
#include <nlohmann/json.hpp>
#include <ASDP_BufferPool.h>
#include <ASDP_SpinFreeQueue.hpp>
#include <ASDP_StreamPacketSortedQueue.h>

using namespace asdp;
using json = nlohmann::json;

Storage_Module_Server::Storage_Module_Server(Storage_Module* parent, uint32_t serialNumber, const std::string& NicName,
    uint16_t sendPort, uint16_t listenPort, uint32_t maxPayloadSize, int verbosity)
  : CoreServerBase(serialNumber, NicName, sendPort, listenPort, maxPayloadSize, verbosity)
  , m_parent(parent)
  , m_replayPaused(false)
  , m_stopReplayThreads(false)
  , m_replayElapsedTime(std::make_shared<ElapsedTimeWithPause>())
{
  // Save the state of record on reset.
  m_recordOnReset = parent->m_persistentState.StoringAtRestart();

  // We start out in "live" mode. This changes when replay is started and stopped.
  // This is true even when we don't have a live conncetion to a parent, according to the spec.
  m_camerasStreaming = true;

  // Add the storage API to our features.
  m_features.push_back(STORAGE_API_AVAILABLE);

  // Record the state message that we will modify and send to clients when we are in idle mode.  If we have
  // stored data, read and store a state message from the first stored stream.  If not, the parent will fill
  // in our stored state after we are constructed.
  std::vector<uint32_t> storedStreamIDs = getStoredStreamIDs();
  if (!storedStreamIDs.empty()) {
    std::string error = ReadInitialTimeAndState(storedStreamIDs[0], m_stateMessage);
    if (!error.empty()) {
      if (m_verbosity >= 0) {
        std::cerr << "Storage_Module_Server::Storage_Module_Server(): readInitialTimeAndState(): " + error << std::endl;
      }
    }
    if (m_stateMessage != nullptr) {
      ConfigureStateFromStoredState();
    }
  }
}

Storage_Module_Server::~Storage_Module_Server()
{
  // Stop the replay if it is running.
  if (m_replaying) {
    doStopReplay(CommandPacketStopReplay(), m_clients[0]);
  }
}

Status Storage_Module_Server::ConfigureStateFromStoredState()
{
  if (m_stateMessage == nullptr) {
    return UNEXPECTED_INTERNAL_STATE;
  }

  // Get the list of features that the server supports.
  Status status = m_stateMessage->GetFeatures(m_features);
  if (status != OKAY) {
    return status;
  }

  return OKAY;
}

std::string Storage_Module_Server::ReadInitialTimeAndState(uint32_t streamID, std::shared_ptr<MessageState>& stateMessage)
{
  // Clear the state message so we can check below for when we have read one.
  stateMessage.reset();

  // Open the main stream file for the stream ID and read the first message from it, storing its time that we
  // will use to offset message times.  Then continue to read messages until we get a status message and use it
  // to set the initial state of the server.
  std::string fileName = m_parent->m_storageRoot + "/" + std::to_string(m_serial) + "/" + std::to_string(streamID) + "/stream0.dat";
  std::shared_ptr<ReceiverFile> replayFile = std::make_shared<ReceiverFile>(fileName);
  if (replayFile->GetConstructorStatus() != OKAY) {
    return "Cannot open " + fileName;
  }
  bool gotFirstTime = false;
  while (!gotFirstTime || (stateMessage == nullptr)) {
    // Get the next packet from the file.
    std::shared_ptr<StreamPacket> packet;
    size_t size = 0;
    Status status = replayFile->ReceiveStreamPacket(1.0, packet, size);
    if (status != OKAY) {
      return "Cannot read state message from " + fileName;
    }

    // Go through any messages in the packet, pulling out a state message if there is one.
    std::shared_ptr<Message> msg;
    status = packet->GetNextMessage(msg);
    while (msg != nullptr) {
      // Get the time and store it for the first message
      if (!gotFirstTime) {
        Time time;
        status = msg->GetTime(time);
        if (status != OKAY) {
          return "Cannot get time from packet";
        }
        m_replayFirstTime = time;
        gotFirstTime = true;
      }

      // Check to see if we have a state message.
      MessageID msgID;
      status = msg->GetType(msgID);
      if (status != OKAY) {
        return "Cannot get message type from packet";
      }
      if (msgID == STATE) {
        stateMessage = std::make_shared<MessageState>(*msg);
        if (stateMessage->GetConstructorStatus() != OKAY) {
          return "Cannot construct state message";
        }
      }
      status = packet->GetNextMessage(msg);
      if (status != OKAY) {
        return "Cannot get next message from packet";
      }
    }
  }

  return "";
}

void Storage_Module_Server::clientBeingRemoved(ClientState& client)
{
  // Remove any subregions that this client has set up on any of the cameras.
  {
    std::unique_lock<std::shared_mutex> lock(m_replayMutex);
    for (const auto& pair : m_subregions) {
      m_subregions[pair.first].clear();
    }
  }

  // Stop replay when the last client is being removed.
  if (m_replaying && (m_clients.size() == 1)) {
    doStopReplay(CommandPacketStopReplay(), client);
  }
}

void Storage_Module_Server::doEveryLoop()
{
  // If the threads are supposed to be stopping, set an error indicating
  // this.
  if (m_parent->m_stop) {
    m_error = "Server is stopping";
  }

  // If we are replaying, check for and handle incoming data.
  auto now = std::chrono::steady_clock::now();
  if (m_replaying) {
    // First update the time code that we should be playing through in a thread-safe way so that
    // all of the image-streaming threads can also make use of it.
    {
      std::shared_lock<std::shared_mutex> lock(m_replayMutex);
      m_streamReplayTime = m_replayFirstTime + m_replayElapsedTime->ElapsedTime();
    }

    // If we don't have a next packet (may be held because it was in the future), read one and find
    // its replay time.
    if (m_replayPacket == nullptr) {
      size_t offset = 0;
      Status status = m_replayFiles[0]->ReceiveStreamPacket(0, m_replayPacket, offset);
      if ((status != OKAY) && (status != TIMEOUT)) {
        m_error = "doEveryLoop(): Error reading replay file: " + ErrorMessage(status);
        return;
      }
      if (status != TIMEOUT) {
        // Read the time from the first message in the packet.
        std::shared_ptr<Message> msg;
        status = m_replayPacket->GetNextMessage(msg);
        if (status != OKAY) {
          m_error = "doEveryLoop(): Error getting message from packet: " + ErrorMessage(status);
          return;
        }
        status = msg->GetTime(m_replayPacketTime);
        if (status != OKAY) {
          m_error = "doEveryLoop(): Error getting time from message: " + ErrorMessage(status);
          return;
        }
      }
    }

    // If the current packet is in the present or past then process it.
    if ((m_replayPacket != nullptr) && (m_replayPacketTime <= m_streamReplayTime)) {

      std::string ret = ForwardPacketToClients(m_replayPacket, true);
      if (!ret.empty()) {
        m_error = "doEveryLoop(): " + ret;
        return;
      }

      // Done with the packet.  We'll look for a new one the next time through.
      m_replayPacket.reset();
    }

    // Flush all messages to the clients.  This may fail because of a closed client.
    for (auto& client : m_clients) {
      Status status = client.m_writer->Flush();
      if (status != OKAY) {
        if (m_verbosity >= 0) {
          std::cerr << "Storage_Module_Server::doEveryLoop(): Error flushing StreamWriter: " << ErrorMessage(status) << std::endl;
          std::cerr << "  (Client may have disconnected)" << std::endl;
        }
      }
    }
  }

  // Once per second, check and fill in the disk-space information in our state.
  if (now - m_lastCheckDiskSpace > std::chrono::seconds(1)) {
    std::filesystem::space_info info = std::filesystem::space(m_parent->m_storageRoot);
    m_remainingDiskSpace = info.available;
    m_totalDiskSpace = info.capacity;
    m_lastCheckDiskSpace = now;
  }
}

void Storage_Module_Server::doReset(const CommandPacketReset& command, ClientState& client)
{
  // Close all client connections
  m_clients.clear();

  // Stop storing and then re-start it if we're supposed to record on reset.
  m_parent->StopStoring();
  if (m_parent->m_persistentState.StoringAtRestart()) {
    m_parent->StartStoring();
  }

  // Reset our clock to count up from zero starting now.
  Time now;
  Status status = m_timer->GetCoreTime(now);
  if (status != OKAY) {
    m_error = "doReset(): Error getting time: " + ErrorMessage(status);
    return;
  }
  status = m_timer->SetCoreNegativeOffset(now);
  if (status != OKAY) {
    m_error = "doReset(): Error setting negative offset: " + ErrorMessage(status);
    return;
  }
}

void Storage_Module_Server::doSetStreamStatePeriod(const CommandPacketSetStreamStatePeriod& command, ClientState& client)
{
  if (CurrentMode() == Storage_Module_Server::Mode::Live) {
    m_parent->SendCommandPacket(command);
  }
}

void Storage_Module_Server::doSetNUCFlagState(const CommandPacketSetNUCFlagState& command, ClientState& client)
{
  if (CurrentMode() == Storage_Module_Server::Mode::Live) {
    m_parent->SendCommandPacket(command);
  }
}

void Storage_Module_Server::doStartOnCameraNUC(const CommandPacketStartOnCameraNUC& command, ClientState& client)
{
  if (CurrentMode() == Storage_Module_Server::Mode::Live) {
    m_parent->SendCommandPacket(command);
  }
}

void Storage_Module_Server::doConfigureTrigger(const CommandPacketConfigureTrigger& command, ClientState& client)
{
  if (CurrentMode() == Storage_Module_Server::Mode::Live) {
    m_parent->SendCommandPacket(command);
  }
}

void Storage_Module_Server::doSoftwareTrigger(const CommandPacketSoftwareTrigger& command, ClientState& client)
{
  if (CurrentMode() == Storage_Module_Server::Mode::Live) {
    m_parent->SendCommandPacket(command);
  }
}

void Storage_Module_Server::doStreamSubregion(const CommandPacketStreamSubregion& command, ClientState& client)
{
  // Fill in the subregion description and endpoint from the command packet.
  SubregionDescription subregion;
  Status status = command.GetRegionDescription(subregion);
  if (status != OKAY) {
    m_error = ErrorMessage(status);
    return;
  }

  StreamEndpoint endpoint;
  status = command.GetEndpoint(endpoint);
  if (status != OKAY) {
    m_error = ErrorMessage(status);
    return;
  }

  // Make a UDP sender for the endpoint and attach it to a writer.
  std::shared_ptr<SenderUDP> sender = std::make_shared<SenderUDP>(endpoint);
  std::shared_ptr<StreamWriter> writer = std::make_shared<StreamWriter>(sender, m_maxPayloadSize);

  // Store the information, locking the mutex while doing so.  If there is already an entry for this
  // endpoint on this camera and client, overwrite it.  If there is not an entry, add it.
  ReplayInfo info;
  info.subregion = subregion;
  info.writer = writer;
  std::unique_lock<std::shared_mutex> lock(m_replayMutex);
  m_subregions[subregion.cameraID][client][endpoint] = info;
}

void Storage_Module_Server::doCancelSubregion(const CommandPacketCancelSubregion& command, ClientState& client)
{
  // Get the camera ID and endpoint from the command packet.
  uint32_t cameraID;
  Status status = command.GetCamera(cameraID);
  if (status != OKAY) {
    m_error = ErrorMessage(status);
    return;
  }
  StreamEndpoint endpoint;
  status = command.GetEndpoint(endpoint);
  if (status != OKAY) {
    m_error = ErrorMessage(status);
    return;
  }

  // Remove any entry, locking the mutex while doing so.  If there is not an entry, ignore that fact.
  std::unique_lock<std::shared_mutex> lock(m_replayMutex);
  m_subregions[cameraID][client].erase(endpoint);
}

void Storage_Module_Server::doStartRecording(const CommandPacketStartRecording& command, ClientState& client)
{
  // If the parent has the same serial number as we do, then try to start storing on it (which will also set our state).
  // Otherwise, we ignore the command because we can only replay and not store.
  if (m_parent->m_serial == m_serial) {
    Status status = m_parent->StartStoring();
    if (status != OKAY) {
      m_error = ErrorMessage(status);
    }
  }
}

void Storage_Module_Server::doStopRecording(const CommandPacketStopRecording& command, ClientState& client)
{
  // If the parent has the same serial number as we do, then try to stop storing on it (which will also set our state).
  // Otherwise, we ignore the command because we can only replay and not store.
  if (m_parent->m_serial == m_serial) {
    Status status = m_parent->StopStoring();
    if (status != OKAY) {
      m_error = ErrorMessage(status);
    }
  }
}

void Storage_Module_Server::doSetStartUpRecordingState(const CommandPacketSetStartUpRecordingState& command, ClientState& client)
{
  std::lock_guard<std::mutex> lock(m_parent->m_storageMutex);
  uint32_t state;
  Status status = command.GetState(state);
  if (status != OKAY) {
    m_error = ErrorMessage(status);
    return;
  }
  m_parent->m_persistentState.SetStoringAtRestart(state != 0);
  if (!m_parent->m_persistentState.SaveToFile()) {
    m_error = "Failed to save persistent-state file";
  }
  m_recordOnReset = m_parent->m_persistentState.StoringAtRestart();
}

std::vector<uint32_t> Storage_Module_Server::getStoredStreamIDs() const
{
  // Find a list of directory names in the storage root directory for our serial number.
  // Select the ones that can be parsed as unsigned integers.
  std::vector<uint32_t> storedStreamIDs;
  std::filesystem::path dirPath = m_parent->m_storageRoot;
  dirPath /= std::to_string(m_serial);
  for (const auto& entry : std::filesystem::directory_iterator(dirPath)) {
    uint32_t streamID = 0;
    try {
      streamID = std::stoul(entry.path().filename().string());
      storedStreamIDs.push_back(streamID);
    }
    catch (...) {
      // Nothing to do here.
    }
  }
  std::sort(storedStreamIDs.begin(), storedStreamIDs.end());
  return storedStreamIDs;
}

void Storage_Module_Server::doListStoredStreams(const CommandPacketListStoredStreams& command, ClientState& client)
{
  std::lock_guard<std::mutex> lock(m_parent->m_storageMutex);

  // Find a list of directory names in the storage root directory for our serial number.
  // Select the ones that can be parsed as unsigned integers.
  std::vector<uint32_t> storedStreamIDs = getStoredStreamIDs();

  // Send the list of stored streams back to the client.
  Status status;
  Time timeCode;
  status = m_timer->GetCoreTime(timeCode);
  if (status != OKAY) {
    m_error = "doListStoredStreams(): Error getting time: " + ErrorMessage(status);
    return;
  }
  for (auto& client : m_clients) {
    std::shared_ptr<StreamPacket> packet;
    status = client.m_writer->GetCurrentPacket(packet);
    if (status != OKAY) {
      m_error = "doListStoredStreams(): Error getting current packet: " + ErrorMessage(status);
      return;
    }

    MessageStoredStreamList message(*packet, timeCode, storedStreamIDs);
    if (message.GetConstructorStatus() != OKAY) {
      m_error = "doListStoredStreams(): Error constructing MessageStoredStreamList: "
        + ErrorMessage(message.GetConstructorStatus())
        + " (client may have disconnected)";
      return;
    }

    // Send the packet.
    status = client.m_writer->Flush();
    if (status != OKAY) {
      if (m_verbosity >= 0) {
        std::cerr << "doListStoredStreams(): Error flushing StreamWriter: " << ErrorMessage(status) << std::endl;
        std::cerr << "  (Client may have disconnected)" << std::endl;
      }
    }
  }
}

void Storage_Module_Server::doEraseAllStoredStreams(const CommandPacketEraseAllStoredStreams& command, ClientState& client)
{
  std::lock_guard<std::mutex> lock(m_parent->m_storageMutex);

  // Find a list of directory names in the storage root directory for our serial number.
  // Select the ones that can be parsed as unsigned integers.
  std::vector<uint32_t> storedStreamIDs;
  std::filesystem::path dirPath = m_parent->m_storageRoot;
  dirPath /= std::to_string(m_serial);
  for (const auto& entry : std::filesystem::directory_iterator(dirPath)) {
    uint32_t streamID = 0;
    try {
      streamID = std::stoul(entry.path().filename().string());
      storedStreamIDs.push_back(streamID);
    } catch (const std::filesystem::filesystem_error& e) {
      // Ignore any errors that occur while deleting the directories.
    }
  }

  // Avoid deleting a stream that is currently being written to.
  // Remove the currently-storing stream ID from the list if there is one.
  if (m_parent->m_writingToID > 0) {
    auto it = std::find(storedStreamIDs.begin(), storedStreamIDs.end(), m_parent->m_writingToID);
    if (it != storedStreamIDs.end()) {
      storedStreamIDs.erase(it);
    }
  }

  // Erase all the stored streams remaining in the list by recursively removing their directory tree.
  for (auto streamID : storedStreamIDs) {
    std::filesystem::path streamPath = dirPath;
    streamPath /= std::to_string(streamID);
    try {
      std::filesystem::remove_all(streamPath);
    } catch (const std::filesystem::filesystem_error& e) {
      // Ignore any errors that occur while deleting the directories.
    }
  }
}

void Storage_Module_Server::doEraseStoredStream(const CommandPacketEraseStoredStream& command, ClientState& client)
{
  std::lock_guard<std::mutex> lock(m_parent->m_storageMutex);

  // Find the ID of the stream to erase.
  uint32_t streamID;
  Status status = command.GetID(streamID);
  if (status != OKAY) {
    m_error = "doListStoredStreams(): " + ErrorMessage(status);
    return;
  }

  // Avoid deleting a stream that is currently being written to (do nothing).
  if (streamID == m_parent->m_writingToID) {
    return;
  }

  // Erase the stored stream by removing its directory tree.
  std::filesystem::path streamPath = m_parent->m_storageRoot;
  streamPath /= std::to_string(m_serial);
  streamPath /= std::to_string(streamID);
  try {
    std::filesystem::remove_all(streamPath);
  } catch (const std::filesystem::filesystem_error& e) {
    // Ignore any errors that occur while deleting the directories.
  }
}

void Storage_Module_Server::doStartReplay(const CommandPacketStartReplay& command, ClientState& client)
{
  // If we're already replaying, then stop replaying first.  Do this before we grab the mutex because
  // the other command will grab it.
  if (m_replaying) {
    doStopReplay(CommandPacketStopReplay(), client);
  }

  std::unique_lock<std::shared_mutex> lock(m_replayMutex);

  // Parse the command packet to get the stream ID to replay and time offset.
  uint32_t streamID;
  Status status = command.GetID(streamID);
  if (status != OKAY) {
    m_error = "doStartReplay(): " + ErrorMessage(status);
    return;
  }
  status = command.GetInitialTime(m_replayInitialTime);
  if (status != OKAY) {
    m_error = "doStartReplay(): " + ErrorMessage(status);
    return;
  }

  // Open the main stream file for the stream ID and read the first message from it, storing its time that we
  // will use to offset message times.  Then continue to read messages until we get a status message and use it
  // to set the initial state of the server.
  std::shared_ptr<MessageState> stateMessage;
  std::string error = ReadInitialTimeAndState(streamID, stateMessage);
  if (stateMessage != nullptr) {
    m_stateMessage = stateMessage;
    ConfigureStateFromStoredState();
  }
  if (!error.empty()) {
    if (m_verbosity >= 0) {
      std::cerr << "Storage_Module_Server::doStartReplay(): " + error << std::endl;
    }
    // Ignore the error and return.  We will not be able to replay without the file.
    m_replayFiles.clear();
    return;
  }
  if (m_verbosity > 1) {
    std::cout << " Storage_Module_Server::Opened replay file for stream: " << streamID << std::endl;
  }
  if (m_verbosity > 3) {
    std::cout << "   First time from replay file: " << m_replayFirstTime.seconds << ":" << m_replayFirstTime.microseconds << std::endl;
  }
  
  // Start replay at the beginning of the file, with offset based on the current steady-clock value.
  m_replayElapsedTime->Reset();

  // Find out how many cameras we have from the state message.
  std::vector<CameraInfo> cameras;
  status = m_stateMessage->GetCameras(cameras);
  if (status != OKAY) {
    if (m_verbosity >= 0) {
      std::cerr << "Storage_Module_Server::doStartReplay(): Cannot get cameras from state message" << std::endl;
    }
    // Ignore the error and return.  We will not be able to replay without the file.
    m_replayFiles.clear();
    return;
  }
  if (m_verbosity > 3) {
    std::cout << "   Number of cameras in replay file: " << cameras.size() << std::endl;
  }
  m_replayThreads.resize(cameras.size() + 1);
  m_replayFiles.resize(cameras.size() + 1);

  // Restart the main stream file so that all packets will be read from it and passed on.
  m_replayFiles[0].reset();
  std::string fileName = m_parent->m_storageRoot + "/" + std::to_string(m_serial) + "/" + std::to_string(streamID) + "/stream0.dat";
  m_replayFiles[0] = std::make_shared<ReceiverFile>(fileName);

  // Open the camera stream files for each stream ID, add them round-robin along with camera IDs to a set of
  // vectors and start the stream replay threads with those vectors.
  m_stopReplayThreads = false;
  static const size_t NUM_REPLAY_THREADS = 9;
  std::vector< std::vector<ReplayCameraDescription> > cameraBatches(NUM_REPLAY_THREADS);
  for (uint32_t i = 1; i <= cameras.size(); i++) {
    ReplayCameraDescription desc;
    desc.cameraID = i;
    std::string fileName = m_parent->m_storageRoot + "/" + std::to_string(m_serial) + "/" + std::to_string(streamID) + "/stream" + std::to_string(i) + ".dat";
    desc.receiver = std::make_shared<ReceiverFile>(fileName);
    cameraBatches[(i-1) % cameraBatches.size()].push_back(desc);
  }
  for (size_t i = 0; i < cameraBatches.size(); i++) {
    m_replayThreads[i] = std::thread(&Storage_Module_Server::ReplayThread, this, cameraBatches[i]);
  }

  // Switching away from live mode and not paused.
  m_replayAtEnd = false;
  m_camerasStreaming = false;
  m_replayPacket.reset();
  m_replaying = true;
  m_replayPaused = false;

  // Get the current time in idle or live mode so we can send the START_OF_REPLAY with it.
  Time nowInLive;
  status = m_timer->GetCoreTime(nowInLive);
  if (status != OKAY) {
    m_error = "Storage_Module_Server::doStartReplay(): Error getting time: " + ErrorMessage(status);
    return;
  }
  if (CurrentMode() == Storage_Module_Server::Mode::Live) {
    m_parent->m_timer->GetCoreTime(nowInLive);
  }

  // Inform the client that we are replaying by sending a START_OF_REPLAY message
  // followed by a clock-sync message.  Use the old time code for the first and the new for
  // the second.
  for (auto& client : m_clients) {
    // Clear the last-sent message times so that we send a new message stream starting now.
    client.m_lastStateSent = { 0, 0 };
    client.m_lastClockSent = { 0, 0 };

    std::shared_ptr<StreamPacket> packet;
    status = client.m_writer->GetCurrentPacket(packet);
    if (status != OKAY) {
      m_error = "Storage_Module_Server::doStartReplay(): Error getting current packet: " + ErrorMessage(status);
      return;
    }

    // The start-of-replay message is sent with the current time code.
    MessageEvent message(*packet, nowInLive, 0, START_OF_REPLAY,
      std::to_string(streamID));
    if (message.GetConstructorStatus() != OKAY) {
      m_error = "Storage_Module_Server::doStartReplay(): Error constructing MessageReplayStarted: " + ErrorMessage(message.GetConstructorStatus());
      return;
    }

    // The clock-sync message is sent with the replay time code.
    MessageEvent message2(*packet, m_replayInitialTime, 0, CLOCK_SYNC, "");
    if (message2.GetConstructorStatus() != OKAY) {
      m_error = "Storage_Module_Server::doStartReplay(): Error constructing MessageClockSync: " + ErrorMessage(message2.GetConstructorStatus());
      return;
    }

    // Send the packet.
    status = client.m_writer->Flush();
    if (status != OKAY) {
      // Client may have disconnected.
      if (m_verbosity >= 0) {
        std::cerr << "Storage_Module_Server::doStartReplay(): Error flushing StreamWriter: " << ErrorMessage(status) << std::endl;
        std::cerr << "  (Client may have disconnected)" << std::endl;
      }
    }
  }
}

void Storage_Module_Server::doPauseReplay(const CommandPacketPauseReplay& command, ClientState& client)
{
  // Do nothing if we're not replaying.
  if (!m_replaying) {
    return;
  }

  m_replayElapsedTime->Pause();
  m_replayPaused = true;

  // Tell all clients that we are paused.
  Status status;
  Time timeCode;
  status = m_timer->GetCoreTime(timeCode);
  if (status != OKAY) {
    m_error = "doPauseReplay(): Error getting time: " + ErrorMessage(status);
    return;
  }
  for (auto& client : m_clients) {
    std::shared_ptr<StreamPacket> packet;
    status = client.m_writer->GetCurrentPacket(packet);
    if (status != OKAY) {
      m_error = "doPauseReplay(): Error getting current packet: " + ErrorMessage(status);
      return;
    }

    MessageEvent message(*packet, timeCode, 0, REPLAY_PAUSED, "");
    if (message.GetConstructorStatus() != OKAY) {
      m_error = "doPauseReplay(): Error constructing MessageStoredStreamList: "
        + ErrorMessage(message.GetConstructorStatus())
        + " (client may have disconnected)";
      return;
    }

    // Send the packet.
    status = client.m_writer->Flush();
    if (status != OKAY) {
      if (m_verbosity >= 0) {
        std::cerr << "doPauseReplay(): Error flushing StreamWriter: " << ErrorMessage(status) << std::endl;
        std::cerr << "  (Client may have disconnected)" << std::endl;
      }
    }
  }
}

void Storage_Module_Server::doResumeReplay(const CommandPacketResumeReplay& command, ClientState& client)
{
  // Do nothing if we're not replaying.
  if (!m_replaying) {
    return;
  }

  m_replayElapsedTime->Resume();
  m_replayPaused = false;

  // Tell all clients that we are resumed.
  Status status;
  Time timeCode;
  status = m_timer->GetCoreTime(timeCode);
  if (status != OKAY) {
    m_error = "doResumeReplay(): Error getting time: " + ErrorMessage(status);
    return;
  }
  for (auto& client : m_clients) {
    std::shared_ptr<StreamPacket> packet;
    status = client.m_writer->GetCurrentPacket(packet);
    if (status != OKAY) {
      m_error = "doResumeReplay(): Error getting current packet: " + ErrorMessage(status);
      return;
    }

    MessageEvent message(*packet, timeCode, 0, REPLAY_RESUMED, "");
    if (message.GetConstructorStatus() != OKAY) {
      m_error = "doResumeReplay(): Error constructing MessageStoredStreamList: "
        + ErrorMessage(message.GetConstructorStatus())
        + " (client may have disconnected)";
      return;
    }

    // Send the packet.
    status = client.m_writer->Flush();
    if (status != OKAY) {
      if (m_verbosity >= 0) {
        std::cerr << "doResumeReplay(): Error flushing StreamWriter: " << ErrorMessage(status) << std::endl;
        std::cerr << "  (Client may have disconnected)" << std::endl;
      }
    }
  }
}

void Storage_Module_Server::doStopReplay(const CommandPacketStopReplay& command, ClientState& client)
{
  // Do nothing if we're not replaying.
  if (!m_replaying) {
    return;
  }

  // Stop all of our per-camera receive threads.
  m_stopReplayThreads = true;
  for (auto& thread : m_replayThreads) {
    // The zeroeth thread will not be joinable because it is not started; the main thread handles it.
    if (thread.joinable()) {
      thread.join();
    }
  }

  // Wait to grab the lock until all threads have stopped because they may be using the mutex.
  std::unique_lock<std::shared_mutex> lock(m_replayMutex);
  m_replayThreads.clear();

  // Stop all of our stream receivers.
  m_replayFiles.clear();

  // Switching back to live (or idle) mode.
  m_camerasStreaming = true;
  m_replayPacket.reset();
  m_replaying = false;

  Time nowInReplay = m_replayFirstTime + m_replayElapsedTime->ElapsedTime();

  // Inform the clients that we are no longer replaying by sending an END_OF_REPLAY message
  // in replay time followed by a clock-sync message in our local time code (if we are idle).
  // The connected server will send clock sync as usual in live mode.
  for (auto& client : m_clients) {
    // Clear the last-sent message times so that we send a new message stream starting now.
    client.m_lastStateSent = { 0, 0 };
    client.m_lastClockSent = { 0, 0 };

    std::shared_ptr<StreamPacket> packet;
    Status status = client.m_writer->GetCurrentPacket(packet);
    if (status != OKAY) {
      m_error = "Storage_Module_Server::doStopReplay(): Error getting current packet: " + ErrorMessage(status);
      return;
    }

    MessageEvent message(*packet, nowInReplay, 0, END_OF_REPLAY, "");
    if (message.GetConstructorStatus() != OKAY) {
      m_error = "Storage_Module_Server::doStopReplay(): Error constructing MessageReplayStopped: " + ErrorMessage(message.GetConstructorStatus());
      return;
    }

    if (CurrentMode() == Storage_Module_Server::Mode::Idle) {
      // The clock-sync message is sent with a current time code.
      Time nowInIdle;
      status = m_timer->GetCoreTime(nowInIdle);
      if (status != OKAY) {
        m_error = "Storage_Module_Server::doStopReplay(): Error getting live time: " + ErrorMessage(status);
        return;
      }
      MessageEvent message2(*packet, nowInIdle, 0, CLOCK_SYNC, "");
      if (message2.GetConstructorStatus() != OKAY) {
        m_error = "Storage_Module_Server::doStopReplay(): Error constructing MessageClockSync: "
          + ErrorMessage(message.GetConstructorStatus());
        return;
      }
    }

    // Send the packet.
    status = client.m_writer->Flush();
    if (status != OKAY) {
      // Client may have disconnected.
      if (m_verbosity >= 0) {
        std::cerr << "Storage_Module_Server::doStopReplay(): Error flushing StreamWriter: "
          + ErrorMessage(status)
          + " (client may have disconnected)";
      }
    }
  }
}

void Storage_Module_Server::ReplayThread(std::vector<ReplayCameraDescription> cameras)
{
  if (m_verbosity > 4) {
    std::cout << "    Storage_Module_Server::ReplayThread(): Starting thread for cameras";
    for (const auto& camera : cameras) {
      std::cout << " " << camera.cameraID;
    }
    std::cout << std::endl;
  }

  // Construct the queue to receive packets from the input thread for each camera.
  std::map<uint32_t, std::shared_ptr< SpinFreeQueue< std::shared_ptr<PacketTime> > > > inputQueues;
  std::map<uint32_t, std::thread> inputThreads;
  for (const auto& camera : cameras) {
    // Construct the queue to receive packets from the input thread for each camera.
    // (This is a shared pointer so that it can be passed to the input thread.)
    inputQueues[camera.cameraID] =
    std::make_shared< SpinFreeQueue< std::shared_ptr<PacketTime> > >();

    // Start the input thread
    inputThreads[camera.cameraID] = std::thread(&Storage_Module_Server::ReplayInputThread, this, camera.receiver, inputQueues[camera.cameraID]);
  }

  // While we're not done, get packets from the input threads and send their messages to the clients of the
  // associated camera when their time has arrived. Busy wait, looping across all cameras.
  std::map<uint32_t, std::shared_ptr<PacketTime> > packetTimes;
  while (!m_stopReplayThreads) {

    for (const auto& camera : cameras) {
      uint32_t cameraID = camera.cameraID;
      std::shared_ptr<PacketTime> &packetTime = packetTimes[cameraID];

      // If we have no cached packet, get one from the input queue if it is available
      if (!packetTime) {
        if (!inputQueues[cameraID]->dequeue(packetTime, std::chrono::milliseconds(0))) {
          continue;
        }
      }

      // If it is time to send the packet, do so.
      if (packetTime && packetTime->elapsedTime <= m_replayElapsedTime->ElapsedTime()) {
        /// @todo Handle skipping packets when there is a specified stride, and don't start sending until the first message is in the past.'
        std::string ret = SendImageStreamPacketToClients(cameraID, packetTime->packet, m_replayFirstTime, m_replayInitialTime);
        if (!ret.empty()) {
          if (m_verbosity >= 0) {
            std::cerr << "Storage_Module_Server::ReplayThread(): Error sending stream packet: " + ret << std::endl;
          }
        }
        packetTime.reset();
      }
    }
  }
  packetTimes.clear();

  // Wait for the input threads to finish.
  for (auto& inputThread : inputThreads) {
    inputThread.second.join();
  }

  if (m_verbosity > 4) {
    std::cout << "    Storage_Module_Server::ReplayThread(): Ending thread for cameras";
    for (const auto& camera : cameras) {
      std::cout << " " << camera.cameraID;
    }
    std::cout << std::endl;
  }
}

void Storage_Module_Server::ReplayInputThread(std::shared_ptr<ReceiverFile> receiver,
  std::shared_ptr< SpinFreeQueue< std::shared_ptr<PacketTime> > > inputQueue)
{
  // Make a pool of packets to use for receiving messages.  When the packet's destructor is called, it
  // will return the memory to the pool.  This speeds up reading because we don't need to allocate a new
  // buffer in the ReceiveStreamPacket() call.  Start with some packets in the pool.  More will be allocated
  // if needed.
  uint32_t numPrefetch = 100;
  asdp::BufferPool bufferPool(m_parent->m_persistentState.TotalBufferSize(), numPrefetch);

  // Use a sorted queue to handle any re-ordering that happened when the packets were stored.
  StreamPacketSortedQueue sortedQueue(50);

  // Receive packets from the receiver (disk) and queue them until the queue has enough entries.
  Status status;
  while (!m_stopReplayThreads) {
    if (inputQueue->awaitEmpty(numPrefetch, std::chrono::milliseconds(100))) {

      // Get the next packet from the receiver.
      std::shared_ptr<StreamPacket> packet;
      size_t offset = 0;
      status = receiver->ReceiveStreamPacket(0.0, packet, offset, bufferPool.GetBuffer());
      if (status == TIMEOUT) {
        continue;
      }
      if (status != OKAY) {
        if (m_verbosity >= 0) {
          std::cerr << "Storage_Module_Server::ReplayInputThread(): Error receiving packet: " << ErrorMessage(status) << std::endl;
        }
        break;
      }

      // Queue the packet and then send all packets that are ready.
      std::list< std::shared_ptr<StreamPacket> > packets = sortedQueue.AddPacket(packet);
      if (packets.size() > 1 && m_verbosity >= 10) {
        std::cout << "Storage_Module_Server::ReplayInputThread(): Re-ordering packets" << std::endl;
      }
      while (!packets.empty()) {
        std::shared_ptr<PacketTime> packetTime(new PacketTime);
        packetTime->packet = packets.front();
        packets.pop_front();

        // Fill in the time, which is the difference between the time in the first message of the packet
        // and the time of the first message that was replayed from this stored data set in the main stream.
        std::shared_ptr<Message> msg;
        status = packetTime->packet->GetNextMessage(msg);
        if ((status != OKAY) || (msg == nullptr)) {
          if (m_verbosity >= 0) {
            std::cerr << "Storage_Module_Server::ReplayInputThread(): Error getting message from packet: " << ErrorMessage(status) << std::endl;
          }
          break;
        }
        asdp::Time msgTime;
        status = msg->GetTime(msgTime);
        if (status != OKAY) {
          if (m_verbosity >= 0) {
            std::cerr << "Storage_Module_Server::ReplayInputThread(): Error getting time from message: " << ErrorMessage(status) << std::endl;
          }
          break;
        }
        asdp::Time elapsed = (msgTime - m_replayFirstTime);
        if (msgTime < m_replayFirstTime) {
          // This stream happened to have a message earlier than the first one in the main stream,
          // clamp it to 0 so it will get played immediately.
          elapsed = 0.0;
        }
        packetTime->elapsedTime = elapsed.seconds + (elapsed.microseconds / 1e6);

        // Queue the packet+time.
        inputQueue->enqueue(packetTime);
      }
    }
  }

  // Remove all of the entries from the queue so that we don't leave any buffers in the pool.
  // This allows the bufferPool object to be destroyed and free all of the memory.
  while (inputQueue->size()) {
    std::shared_ptr<PacketTime> packetTime;
    inputQueue->dequeue(packetTime, std::chrono::milliseconds(100));
  }
}

Storage_Module_Server::Mode Storage_Module_Server::CurrentMode() const
{
  if (m_replaying) {
    return Storage_Module_Server::Mode::Replaying;
  }
  if ((m_parent->m_stream != nullptr) && (m_parent->m_serial == m_serial)) {
    return Storage_Module_Server::Mode::Live;
  }
  return Storage_Module_Server::Mode::Idle;
}

Status Storage_Module_Server::SendStateMessage(ClientState& client)
{
  // If we are idling (neither replaying nor streaming live), then we generate state messages
  // by adjusting the time and adding features to our stored one.  Otherwise, we don't send
  // them because we will be forwarding them from one or the other incoming stream.
  if (CurrentMode() == Storage_Module_Server::Mode::Idle) {
    Time time;
    Status status = m_timer->GetCoreTime(time);
    if (status != OKAY) {
      return status;
    }
    return SendModifiedStateMessage(m_stateMessage, time, client);
  }

  return OKAY;
}

Status Storage_Module_Server::SendClockSyncMessage(ClientState& client)
{
  // If we are idling (neither replaying nor streaming live), then we use the base class method
  // to send clock sync messages.  Otherwise, we don't send them because we will be forwarding them
  // from one or the other incoming stream.
  if (CurrentMode() == Storage_Module_Server::Mode::Idle) {
    return CoreServerBase::SendClockSyncMessage(client);
  }

  return OKAY;
}

Status Storage_Module_Server::SendModifiedStateMessage(std::shared_ptr<MessageState> original, Time timeCode, ClientState& client)
{
  if (m_verbosity >= 10) {
    std::cout << "  Sending modified state message" << std::endl;
  }

  // Get some values from our state and the rest of the values from the original message.
  uint8_t storing = m_storing;
  uint8_t camerasStreaming = m_camerasStreaming;
  uint8_t replaying = m_replaying;
  uint8_t replayAtEnd = m_replayAtEnd;
  uint8_t recordOnReset = m_parent->m_persistentState.StoringAtRestart();
  uint64_t totalDiskSpace = m_totalDiskSpace;
  uint64_t remainingDiskSpace = m_remainingDiskSpace;
  Time streamReplayTime = m_streamReplayTime;

  std::vector<FeatureID> features;
  std::vector<CameraInfo> cameras;
  uint32_t numTemperaturesPerCamera;
  uint32_t numSystemTemperatures;
  std::vector<TriggerInfo> triggers;

  Status status;
  if (original != nullptr) {
    status = original->GetFeatures(features);
    if (status != OKAY) {
      return status;
    }
    status = original->GetCameras(cameras);
    if (status != OKAY) {
      return status;
    }
    status = original->GetNumTempSensorsPerCamera(numTemperaturesPerCamera);
    if (status != OKAY) {
      return status;
    }
    status = original->GetNumExternalTempSensors(numSystemTemperatures);
    if (status != OKAY) {
      return status;
    }
    status = original->GetTriggerConfigs(triggers);
    if (status != OKAY) {
      return status;
    }

    // If the specified time code is zero, read the time from the original message instead.
    if (timeCode.seconds == 0 && timeCode.microseconds == 0) {
      status = original->GetTime(timeCode);
      if (status != OKAY) {
        return status;
      }
    }
  }

  // Add the storage feature to the list of features if it is not in there.
  if (find(features.begin(), features.end(), STORAGE_API_AVAILABLE) == features.end()) {
    features.push_back(STORAGE_API_AVAILABLE);
  }

  // Find the current packet and construct a state message with the filled-in values.
  std::shared_ptr<StreamWriter> writer = client.m_writer;
  std::shared_ptr<StreamPacket> packet;
  status = writer->GetCurrentPacket(packet);
  if (status != OKAY) {
    return status;
  }

  // Construct a state message with the filled-in values.
  MessageState message(*packet, timeCode,
    features, cameras,
    numTemperaturesPerCamera, numSystemTemperatures,
    storing, camerasStreaming, replaying, replayAtEnd,
    recordOnReset,
    triggers,
    totalDiskSpace, remainingDiskSpace,
    streamReplayTime);
  if (message.GetConstructorStatus() != OKAY) {
    // Retry after flushing the buffer.
    status = writer->Flush();
    if (status != OKAY) {
      return status;
    }
    status = writer->GetCurrentPacket(packet);
    if (status != OKAY) {
      return status;
    }
    message = MessageState(*packet, timeCode,
      features, cameras,
      numTemperaturesPerCamera, numSystemTemperatures,
      storing, camerasStreaming, replaying, replayAtEnd,
      recordOnReset,
      triggers,
      totalDiskSpace, remainingDiskSpace,
      streamReplayTime);
    status = message.GetConstructorStatus();
    if (status != OKAY) {
      return status;
    }
  }

  return OKAY;
}

std::string Storage_Module_Server::ForwardPacketToClients(std::shared_ptr<StreamPacket> packet, bool adjustTime)
{
  // Go through all the messages in the packet, adjust time, and send them to the clients if appropriate.
  std::shared_ptr<Message> msg;
  Status status = packet->GetNextMessage(msg);
  while (msg != nullptr) {
    // Find out the time of the message.
    Time time;
    status = msg->GetTime(time);
    if (status != OKAY) {
      return "ForwardPacketToClients(): Error getting time from message: " + ErrorMessage(status);
    }

    // Get the message ID so we can know if we need to squeltch it.
    MessageID msgID;
    status = msg->GetType(msgID);
    if (status != OKAY) {
      return "ForwardPacketToClients(): Error getting message type: " + ErrorMessage(status);
    }

    // If we've been asked to, adjust the time of the message to match the replay time base.
    if (adjustTime) {
      time += m_replayInitialTime;
      // Never wrap around to negative time.
      if (time >= m_replayFirstTime) {
        time -= m_replayFirstTime;
      } else {
        time = Time(0, 0);
      }
    } else {
      // Default of zero re-uses the original message time, both for the modified state
      // message and for the copy to stream packet.
      time = Time();
    }

    // Send to all clients.
    for (auto& client : m_clients) {

      // If this is a state message, then we need to adjust it and send it to the client.
      // We modify its time by the offsets, add the storage feature, and send it to the client.
      if (msgID == STATE) {
        Status status = SendModifiedStateMessage(std::static_pointer_cast<MessageState>(msg), time, client);
        if (status != OKAY) {
          return "ForwardPacketToClients(): Error sending modified state message: " + ErrorMessage(status);
        }
        continue;
      }

      // Determine whether we want to squelch because they are ones that we should not forward.
      std::vector<MessageID> squelchTypes = { };
      if (!client.m_streamingPoses) {
        squelchTypes.push_back(POSE);
      }
      if (!client.m_streamingTemperatures) {
        squelchTypes.push_back(TEMPERATURE);
      }

      // If we don't want to squelch this message, then add it to the packet.
      if (std::find(squelchTypes.begin(), squelchTypes.end(), msgID) == squelchTypes.end()) {
        std::shared_ptr<StreamPacket> clientPacket;
        status = client.m_writer->GetCurrentPacket(clientPacket);
        if (status != OKAY) {
          return "ForwardPacketToClients(): Error getting current packet: " + ErrorMessage(status);
        }
        status = msg->CopyToStreamPacket(*clientPacket, time);
        if (status != OKAY) {
          return "ForwardPacketToClients(): Error adding message to packet: " + ErrorMessage(status);
        }
      }
    }

    // Get the next message in the packet.
    status = packet->GetNextMessage(msg);
    if (status != OKAY) {
      return "ForwardPacketToClients(): Error getting next message: " + ErrorMessage(status);
    }
  }

  return "";
}

std::string Storage_Module_Server::SendImageStreamPacketToClients(uint32_t cameraID,
  std::shared_ptr<StreamPacket> packet, Time subtractTime, Time AddTime)
{
  // Find the total offset time for the replay in fractional seconds.
  double offsetTime;
  if (AddTime >= subtractTime) {
    Time delta = AddTime - subtractTime;
    offsetTime = delta.seconds + (delta.microseconds / 1e6);
  } else {
    Time delta = subtractTime - AddTime;
    offsetTime = -(delta.seconds + (delta.microseconds / 1e6));
  }

  // Offset the time of all messages in the packet by the offset time.
  Status status = packet->OffsetMessageTimes(offsetTime);
  if (status != OKAY) {
    return "Error offsetting message times: " + ErrorMessage(status);
  }

  // Insert the message into the stream for all endpoints on each client for this camera.
  std::map<ClientState, std::map<StreamEndpoint, ReplayInfo> > myReplayInfo;
  {
    std::shared_lock<std::shared_mutex> lock(m_replayMutex);
    myReplayInfo = m_subregions[cameraID];
  }
  for (const auto& clientMapsPair : myReplayInfo) {
    const ClientState& client = clientMapsPair.first;
    for (const auto& subregionMapsPair : clientMapsPair.second) {
      const StreamEndpoint& endpoint = subregionMapsPair.first;
      const ReplayInfo& info = subregionMapsPair.second;

      // Send the packet.
      status = info.writer->InsertPacket(*packet);
      if (status != OKAY) {
        return "Error sending packet: " + ErrorMessage(status) + " (Client may have disconnected)";
      }
    }
  }

  // No errors.
  return "";
}

Storage_Module::Storage_Module(const std::string& NicNameIn, const std::string& NicNameOut,
                               const std::string StorageRoot, int verbosity)
  : CoreClient(NicNameIn)
  , m_status(CoreClient::GetConstructorStatus())
  , m_verbosity(verbosity)
  , m_nicNameIn(NicNameIn)
  , m_nicNameOut(NicNameOut)
  , m_storageRoot(StorageRoot)
  , m_numCameras(0)
  , m_writingToID(0)
  , m_persistentState(StorageRoot + "/config.json")
  , m_stop(false)
  , m_nextPort(10101)
{
  if (m_status != OKAY) {
    return;
  }

  // Verify that the storage root directory exists.
  std::filesystem::path configPath = m_storageRoot;
  if (!std::filesystem::exists(configPath)) {
    m_status = FILE_FAILURE;
    return;
  }

  // Verify that the buffer sizes in the configuration file are valid.
  if (m_persistentState.DiskBlockSize() < 256) {
    if (m_verbosity >= 0) {
      std::cerr << "Storage_Module::Disk block size is too small" << std::endl;
    }
    m_status = UNEXPECTED_INTERNAL_STATE;
    return;
  }
  if (m_persistentState.TotalBufferSize() < m_persistentState.HighWaterMark() + 9000) {
    if (m_verbosity >= 0) {
      std::cerr << "Storage_Module::High water mark is too small relative to total buffer size" << std::endl;
    }
    m_status = UNEXPECTED_INTERNAL_STATE;
    return;
  }
  if (m_persistentState.TotalBufferSize() < 2 * m_persistentState.DiskBlockSize()) {
    if (m_verbosity >= 0) {
      std::cerr << "Storage_Module::Total buffer size is too small relative to disk buffer size" << std::endl;
    }
    m_status = UNEXPECTED_INTERNAL_STATE;
    return;
  }

  // Start a server thread for each serial number directory found in the storage root.
  // Use the factory function to determine the listening port for each server.
  for (const auto& entry : std::filesystem::directory_iterator(m_storageRoot)) {
    if (entry.is_directory()) {
      // If we can't convert the name into an unsigned integer, skip it.
      uint32_t serialNumber = 0;
      try {
        serialNumber = std::stoul(entry.path().filename().string());
        if (verbosity > 1) {
          std::cout << " Storage_Module::Starting server for serial# " << serialNumber << std::endl;
        }
        m_servers.emplace_back(
          std::make_shared<ServerInfo>(
            serialNumber,
            std::make_shared<Storage_Module_Server>(this, serialNumber, NicNameOut,
              10102, m_nextPort.fetch_sub(1), 9000 - 28, verbosity)));
        m_server_threads.emplace_back(std::thread(&Storage_Module::ServerThread, this, m_servers.back()));
      } catch(...) {
        // Nothing to do here.
      }
    }
  }

  // Start the client thread
  m_client_thread = std::thread(&Storage_Module::ClientThread, this);

  if (m_verbosity > 0) {
    std::cout << "Storage_Module::Storage_Module() constructed" << std::endl;
  }
}

Storage_Module::~Storage_Module()
{
  if (m_verbosity > 0) {
    std::cout << "Storage_Module::~Storage_Module() stopping" << std::endl;
  }

  // Stop all threads and wait for them to finish
  m_stop = true;

  for (auto& thread : m_server_threads) {
    if (thread.joinable()) {
      thread.join();
    }
  }
  m_server_threads.clear();
  if (m_client_thread.joinable()) {
    m_client_thread.join();
  }

  for (auto& thread : m_receiver_threads) {
    if (thread.joinable()) {
      thread.join();
    }
  }
  m_receiver_threads.clear();

  if (m_verbosity > 0) {
    std::cout << "Storage_Module::~Storage_Module() destroyed" << std::endl;
  }
}

Status Storage_Module::GetCurrentStatus() const
{
  return m_status;
}

void Storage_Module::ServerThread(std::shared_ptr<ServerInfo> server)
{
  // Run the server main loop. That will internally call the doEveryLoop() function,
  // which will notify the loop that it is time to quit when the m_stop flag is set.
  std::string ret = server->m_server->run();
  if (!m_stop) {
    // If we're not stopping, then we had an error.
    if (m_verbosity >= 0) {
      std::cerr << "Storage_Module::ServerThread() run completed before stopping with error: " << ret << std::endl;
    }
    m_status = UNEXPECTED_INTERNAL_STATE;
  }
}

std::shared_ptr<Message> Storage_Module::WaitForMessageType(MessageID type, float seconds)
{
  std::shared_ptr<Message> empty;   ///< We return this on failure.
  std::chrono::high_resolution_clock::time_point start = std::chrono::high_resolution_clock::now();
  do {
    std::shared_ptr<StreamPacket> response;
    size_t offset = 0;
    Status status = m_stream->ReceiveStreamPacket(0, response, offset);
    if ((status != OKAY) && (status != TIMEOUT)) {
      return empty;
    }
    if (response != nullptr) {
      std::shared_ptr<Message> message;
      status = response->GetNextMessage(message);
      if (status != OKAY) {
        return empty;
      }
      while (message != nullptr) {
        MessageID messageType;
        status = message->GetType(messageType);
        if (status != OKAY) {
          return empty;
        }
        if (messageType == type) {
          // Worked!
          return message;
        }
        status = response->GetNextMessage(message);
        if (status != OKAY) {
          return empty;
        }
      }
    }
  } while (std::chrono::duration<double>(std::chrono::high_resolution_clock::now() - start).count() <= seconds);

  return empty;
}

void Storage_Module::ClientThread()
{
  bool reportedError = false;

  while (!m_stop) {

    // If we are already connected to a server, check for and handle incoming data.
    if (m_stream != nullptr) {

      /// Read all incoming messages on the command channel and store them to the log file and/or
      /// forward them to a server that is handling this client as appropriate.
      std::shared_ptr<StreamPacket> response;
      size_t offset = 0;
      Status status = m_stream->ReceiveStreamPacket(0, response, offset);
      if ((status != OKAY) && (status != TIMEOUT)) {
        if (m_verbosity > 1) {
          std::cout << " Storage_Module::Disconnected from server with serial# " << m_serial << std::endl;
        }

        // Drop the connection and we'll automatically try to reconnect later.
        std::lock_guard<std::mutex> lock(m_storageMutex);
        m_stream.reset();
        m_server->m_storing = false;
        m_storageSenders.clear();
        for (auto& sender : m_storageSenders) {
          sender.reset();
        }
        continue;
      }
      if (response != nullptr) {

        // Grab the storage file from element 0 in the storage-senders vector, which is the non-camera stream.
        // Hold the storage mutex while we do so.  We keep the shared_ptr to the sender while we're using it,
        // so it will persist even if the element is reset.
        std::shared_ptr<SenderFile> sender;
        {
          std::lock_guard<std::mutex> lock(m_storageMutex);
          if (m_storageSenders.size() > 0) {
            sender = m_storageSenders[0];
          }
        }

        // If we have a valid sender, write the packet to the file.  We ignore the return
        // status -- if the disk fills up we'll just keep trying and failing to write.
        if (sender != nullptr) {
          status = sender->SendStreamPacket(*response);
          if ((status != OKAY) && (m_verbosity >= 0)) {
            if (!reportedError) {

              std::cerr << "Storage_Module::Failed to write packet to storage file: " << ErrorMessage(status) << std::endl
                << " (Disk full?  No further errors to write will be reported)" << std::endl;
              reportedError = true;
            }
          }
        }

        // When our associated server object is live, forward the message through it.
        // Adjust any state messages.
        // Don't foward pose or temperature messages if we were not asked to.
        if (m_server->CurrentMode() == Storage_Module_Server::Mode::Live) {

          // Do not change the time of the message, as it is already in the correct time base.
          std::string ret = m_server->ForwardPacketToClients(response, false);
          if (!ret.empty()) {
            if (m_verbosity >= 0) {
              std::cerr << "Storage_Module::ClientThread() error forwarding packet to clients: " << ret << std::endl;
              std::cerr << "   (Client may have disconnected)" << std::endl;
            }
          }

          // Flush all of the clients' buffers.
          for (auto& client : m_server->m_clients) {
            status = client.m_writer->Flush();
            if (status != OKAY) {
              if (m_verbosity >= 0) {
                std::cerr << "Storage_Module::ClientThread() error flushing StreamWriter: " << ErrorMessage(status) << std::endl;
                std::cerr << "   (Client may have disconnected)" << std::endl;
              }
            }
          }
        }
      }

    } else {
      // We are not connected to a server, so look for one in the list of identified servers.
      // The Discovery thread is already running, so we don't need to start it here.
      // We watch for a server to show up in the discovery list, then we connect to it.
      std::vector<std::string> servers;
      Status status = IdentifiedServers(servers);
      if (status != OKAY) {
        // We're broken, so we can't do anything else.  Just set the status and return.
        m_status = status;
        return;
      }
      if (servers.size() > 0) {
        // We found a server, so connect to it.
        uint16_t major, minor, patch;
        Status status = ConnectToServer(servers[0], major, minor, patch);
        if (status != OKAY) {
          // Could not connect to the server.  Skip this round and try again later.
          std::this_thread::sleep_for(std::chrono::milliseconds(100));
          continue;
        }
        if (m_verbosity > 1) {
          std::cout << " Storage_Module::Connected to server " << servers[0] << ", serial# " << m_serial << std::endl;
        }

        // See if we already have a server with this serial number in our list of servers.
        std::shared_ptr<Storage_Module_Server> server;
        for (auto s : m_servers) {
          if (s->m_serialNumber == m_serial) {
            server = s->m_server;
            break;
          }
        }
        if (server == nullptr) {
          // No server, create a new subdirectory for it and start a server thread.
          // Fill it into the server that we're using.
          Status status = ConstructNewServer(server);
          if (status != OKAY) {
            // We're broken, so we can't do anything else.  Just set the status and return.
            m_status = status;
            return;
          }
        }

        // Keep track of this server as the one we forward data to.
        m_server = server;

        // Get the information about the server we just connected to.  Use it to set up the incoming
        // streams from each camera.  Also use it to request streaming of all optional features that
        // it supports.  Also use it to configure our server state.
        std::shared_ptr<Message> response = WaitForMessageType(STATE, 2.0);
        if (response == nullptr) {
          // We're broken, so we can't do anything else.  Just set the status and return.
          m_status = TIMEOUT;
          return;
        }
        if (m_verbosity > 2) {
          std::cout << "  Storage_Module::Got state" << std::endl;
        }
        MessageState state(*response);
        status = state.GetConstructorStatus();
        if (status != OKAY) {
          // We're broken, so we can't do anything else.  Just set the status and return.
          m_status = status;
          return;
        }
        status = ConfigureClientConnection(state);
        if (status != OKAY) {
          // We're broken, so we can't do anything else.  Just set the status and return.
          m_status = status;
          return;
        }

        // Store the state message for the server to use to generate in case it becomes idle because
        // our connection drops.
        m_server->m_stateMessage = std::make_shared<MessageState>(state);
        if (m_server->m_stateMessage != nullptr) {
          m_server->ConfigureStateFromStoredState();
        }


        // See if recording at start-up is enabled for this server.  If so, create the structures that
        // will be used for writing as if we had received the command to start recording.
        if (m_persistentState.StoringAtRestart()) {
          status = StartStoring();
          if (status != OKAY) {
            // We're broken, so we can't do anything else.  Just set the status and return.
            m_status = status;
            return;
          }
        }

      } else {
        // Sleep briefly to keep from hogging the CPU while we're not connected.
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
      }
    }
  }
}

Status Storage_Module::StartStoring()
{
  std::lock_guard<std::mutex> lock(m_storageMutex);

  // Ensure that we are connected to a server before we start storing (and that we have storage server pointers).
  if ((m_stream == nullptr) || (m_storageSenders.size() == 0)) {
    return OKAY;
  }

  // Create the appropriate directory to store our files into by finding the lowest unused ID (starting with 1)
  // that is available in the root directory under our serial number.
  uint32_t storageID = 1;
  while (std::filesystem::exists(m_storageRoot + "/" + std::to_string(m_serial) + "/" + std::to_string(storageID))) {
    storageID++;
  }
  std::string dirName = m_storageRoot + "/" + std::to_string(m_serial) + "/" + std::to_string(storageID);
  if (!std::filesystem::create_directory(dirName)) {
    if (m_verbosity >= 0) {
      std::cerr << "Storage_Module::Failed to create directory: "
        << ErrorMessage(m_storageSenders[0]->GetConstructorStatus()) << std::endl;
      std::cerr << "  Directory name: " << dirName << std::endl;
    }
    return FILE_FAILURE;
  }

  // Make a file storage sender for the non-camera stream.  This is not writing in DirectMode.
  std::string fileName = m_storageRoot + "/" + std::to_string(m_serial) + "/" + std::to_string(storageID) + "/stream0.dat";
  m_storageSenders[0] = std::make_shared<SenderFile>(fileName, false);
  if (m_storageSenders[0]->GetConstructorStatus() != OKAY) {
    if (m_verbosity >= 0) {
      std::cerr << "Storage_Module::Failed to open storage file for non-camera stream: "
        << ErrorMessage(m_storageSenders[0]->GetConstructorStatus()) << std::endl;
      std::cerr << "  File name: " << fileName << std::endl;
    }
    return FILE_FAILURE;
  } else if (m_verbosity > 1) {
    std::cout << " Storage_Module::Opened storage file for non-camera stream: " << fileName << std::endl;
  }

  // Make a file storage sender for each camera stream.  These are writing in DirectMode.
  for (uint32_t i = 1; i <= m_numCameras; i++) {
    fileName = m_storageRoot + "/" + std::to_string(m_serial) + "/" + std::to_string(storageID) + "/stream" + std::to_string(i) + ".dat";
    m_storageSenders[i] = std::make_shared<SenderFile>(fileName, true);
    if (m_storageSenders[i]->GetConstructorStatus() != OKAY) {
      if (m_verbosity >= 0) {
        std::cerr << "Storage_Module::Failed to open storage file for camera " + std::to_string(i) << " stream: "
          << ErrorMessage(m_storageSenders[i]->GetConstructorStatus()) << std::endl;
        std::cerr << "  File name: " << fileName << std::endl;
      }
      return FILE_FAILURE;
    } else if (m_verbosity > 1) {
      std::cout << " Storage_Module::Opened storage file for camera stream: " << fileName << std::endl;
    }
  }

  // Record where we are storing to.
  m_writingToID = storageID;

  // Set the flag that we are storing in our server.
  m_server->m_storing = true;

  return OKAY;
}

Status Storage_Module::StopStoring()
{
  std::lock_guard<std::mutex> lock(m_storageMutex);

  // Close all the storage senders.
  for (auto &sender : m_storageSenders) {
    sender.reset();
  }

  // We are not storing.
  m_writingToID = 0;

  // Set the flag that we are not storing in our server.
  m_server->m_storing = false;

  return OKAY;
}

Status Storage_Module::ConstructNewServer(std::shared_ptr<Storage_Module_Server>& server)
{
  if (m_verbosity > 1) {
    std::cout << " Storage_Module::ConstructNewServer() for serial# " << m_serial << std::endl;
  }

  // Make a directory for the serial number in the storage root.
  std::filesystem::path dirPath = m_storageRoot;
  dirPath /= std::to_string(m_serial);
  if (std::filesystem::exists(dirPath)) {
    // This should not happen.
    return UNEXPECTED_INTERNAL_STATE;
  }
  std::filesystem::create_directory(dirPath);

  // Create a new server and start a server thread for it.
  m_servers.emplace_back(
    std::make_shared<ServerInfo>(
      m_serial,
      std::make_shared<Storage_Module_Server>(this, m_serial, m_nicNameOut,
        10102, m_nextPort.fetch_sub(1), 9000 - 28, m_verbosity)));

  server = m_servers.back()->m_server;

  return OKAY;
}

Status Storage_Module::ConfigureClientConnection(const MessageState& response)
{
  // Get the list of features that the server supports.
  std::vector<FeatureID> features;
  Status status = response.GetFeatures(features);
  if (status != OKAY) {
    return status;
  }

  // Enable streaming for each of them that we can receive.
  for (auto feature : features) {
    switch (feature) {
      case TEMPERATURE_API_AVAILABLE:
        status = SendCommandPacket(CommandPacketStreamTemperatures());
        if (status != OKAY) {
          return status;
        }
        break;
      case POSE_API_ORIENTATION_AVAILABLE:
      case POSE_API_POSITION_AVAILABLE:
        status = SendCommandPacket(CommandPacketStreamPoses());
        if (status != OKAY) {
          return status;
        }
        break;
      default:
        // We never heard of this feature, so we can't enable it.
        if (m_verbosity >= 0) {
          std::cerr << "Storage_Module::Unknown feature ID: " << feature << std::endl;
        }
        return UNEXPECTED_INTERNAL_STATE;
    }
  }

  // Get a list of the cameras that the server supports.
  std::vector<CameraInfo> cameras;
  status = response.GetCameras(cameras);
  if (status != OKAY) {
    return status;
  }
  m_numCameras = cameras.size();

  // Make sure that we have a storage sender entry for every camera stream (by ID) and for the non-camera stream (stream 0).
  m_storageSenders.resize(m_numCameras + 1);

  // Open a UDP stream receiver for each camera in the system, connecting it
  // to a thread that will receive and route the data to storage and/or a connected
  // client depending on our mode of operation.
  uint32_t whichCamera = 0;
  for (auto &camera : cameras) {
    // Next camera index.
    whichCamera++;

    // Open a UDP stream receiver for this camera.
    std::shared_ptr<ReceiverUDP> stream = std::make_shared<ReceiverUDP>(m_nicNameIn);
    if (stream->GetConstructorStatus() != OKAY) {
      return stream->GetConstructorStatus();
    }

    // Start the stream receiver thread to listen on this receiver.
    std::shared_ptr<ReceiverInfo> receiverInfo = std::make_shared<ReceiverInfo>(stream, whichCamera);
    m_receiver_threads.emplace_back(std::thread(&Storage_Module::StreamReceiverThread, this, receiverInfo));
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Find the minimum period for the camera and which internal trigger ID it uses, then
    // configure the trigger to run at that rate.
    TriggerInfo ti;
    ti.ID = camera.trigger;
    ti.mode = 1;
    ti.period = camera.minTriggerPeriod;
    ti.offset = 0;
    ti.trackingFactor = 0.5;
    status = SendCommandPacket(CommandPacketConfigureTrigger(ti));
    if (status != OKAY) {
      return status;
    }
    if (m_verbosity > 3) {
      std::cout << "   Configured trigger for camera " << whichCamera << " with period " << ti.period << " seconds" << std::endl;
    }

    // Request the server to start streaming data from this camera.
    SubregionDescription subregion;
    subregion.cameraID = whichCamera;
    subregion.skipFrames = 0;
    subregion.startTimeSeconds = 0;
    subregion.startTimeMicroseconds = 0;
    subregion.left = 0;
    subregion.top = 0;
    subregion.right = camera.width - 1;
    subregion.bottom = camera.height - 1;
    
    uint16_t port;
    status = stream->GetPort(port);
    if (status != OKAY) {
      return status;
    }
    StreamEndpoint endpoint(m_nicNameIn, port);

    status = SendCommandPacket(CommandPacketStreamSubregion(endpoint, subregion));
    if (status != OKAY) {
      return status;
    }
  }

  /// @todo We may want two triggers when we have stereo cameras along with narrow-field cameras.

  return OKAY;
}

/// @brief Structure to hold data describing writing a buffer to disk.
struct WriteBufferInfo {
  std::shared_ptr<SenderFile> sender;
  std::shared_ptr<std::vector<uint8_t>> buffer;
  size_t bytesToWrite = 0;
};

/// @brief Helper function to run as a thread that writes data to disk.
static void WriteBuffersToFile(asdp::SpinFreeQueue<WriteBufferInfo>& writeQueue, std::atomic<bool>& stop)
{
  while (!stop) {
    // We need this to be destroyed every time through the loop so we release its shared pointer,
    // which will cause the SenderFile to be deleted if it is the last reference to it.
    WriteBufferInfo info;
    if (writeQueue.dequeue(info, std::chrono::milliseconds(100))) {
      info.sender->Send(info.buffer->data(), info.bytesToWrite);
    }
  }

  // Drain the queue before we return.
  while (writeQueue.size()) {
    WriteBufferInfo info;
    writeQueue.dequeue(info, std::chrono::milliseconds(100));
  }
}

void Storage_Module::StreamReceiverThread(std::shared_ptr<ReceiverInfo> receiver)
{
  if (m_verbosity > 3) {
    std::cout << "   Storage_Module::StreamReceiverThread() started" << std::endl;
  }

  // Pool of buffers for receiving data and writing it to disk.  We pre-allocate them here to
  // avoid the overhead of creating and destroying them at run time.  We pre-allocate a bunch,
  // but more will be created as needed.
  asdp::BufferPool bufferPool(m_persistentState.TotalBufferSize(), 100);

  // Start another thread that will be responsible for writing the data to disk, constructing the queue
  // we will use to send to it and an atomic Boolean that will tell it when to stop.
  asdp::SpinFreeQueue<WriteBufferInfo> writeQueue;
  std::atomic<bool> stop(false);
  std::thread writeThread(WriteBuffersToFile, std::ref(writeQueue), std::ref(stop));

  // Currently-used buffer and the number of bytes in it.
  std::shared_ptr<std::vector<uint8_t>> buffer = bufferPool.GetBuffer();
  size_t bytesInBuffer = 0;

  // Keep track of the previously-used stream writer so we can tell when it changes.
  // It is initially set to nullptr so that we discard packets until storing is started.
  std::shared_ptr<SenderFile> previousSender;
  std::shared_ptr<SenderFile> currentSender;

  // Keep track of the previous sequence number and report if there is a gap
  uint32_t previousSequenceNumber = 0;

  while (!m_stop) {

    // See if we have changed to a new stream writer.  If so, flush the current buffer to disk and
    // get a new buffer from the pool.
    {
      std::lock_guard<std::mutex> lock(m_storageMutex);
      currentSender = m_storageSenders[receiver->m_ID];
    }
    if (currentSender != previousSender) {
      // Only write if there is an actual writer and there is data in the buffer.
      if ((previousSender != nullptr) && (bytesInBuffer > 0)) {
        // Pad with zeroes to an even number of block sizes.
        while ((bytesInBuffer < buffer->size()) && (bytesInBuffer % m_persistentState.DiskBlockSize() != 0)) {
          (*buffer)[bytesInBuffer] = 0;
          bytesInBuffer++;
        }

        // Write
        WriteBufferInfo info;
        info.sender = previousSender;
        info.buffer = buffer;
        info.bytesToWrite = bytesInBuffer;
        writeQueue.enqueue(info);
      }
      buffer = bufferPool.GetBuffer();
      bytesInBuffer = 0;
      previousSender = currentSender;
      previousSequenceNumber = 0;
    }

    // Get the next packet from the stream, adding it to the end of our existing buffer.
    // Time out after 1 ms so we can check for a stop condition.
    std::shared_ptr<StreamPacket> packet;
    Status status = receiver->m_receiver->ReceiveStreamPacket(1e-3, packet, bytesInBuffer, buffer);

    // Check the sequence number for a gap.
    if (status == OKAY) {
      uint32_t sequenceNumber;
      status = packet->GetSequenceNumber(sequenceNumber);
      if (status == OKAY) {
        if ((previousSequenceNumber != 0) && (sequenceNumber != previousSequenceNumber + 1)) {
          if (m_verbosity >= 0) {
            std::cerr << "Storage_Module::StreamReceiverThread() gap in sequence numbers: " << previousSequenceNumber << " to " << sequenceNumber << std::endl;
          }
        }
        previousSequenceNumber = sequenceNumber;
      }
    }

    // See if we've reached the high water mark for the buffer.  If so, copy the remaining bytes
    // past the last full disk block into a new buffer and then write the full-block-sized portion
    // of the old buffer to disk.
    if (bytesInBuffer >= m_persistentState.HighWaterMark()) {
      // Copy the remaining bytes into a new buffer.
      std::shared_ptr<std::vector<uint8_t>> newBuffer = bufferPool.GetBuffer();
      uint32_t fullBlocks = m_persistentState.DiskBlockSize() * (bytesInBuffer / m_persistentState.DiskBlockSize());
      if (fullBlocks < bytesInBuffer) {
        std::copy(buffer->data() + fullBlocks, buffer->data() + bytesInBuffer, newBuffer->data());
      }
      bytesInBuffer -= fullBlocks;

      // Write the full blocks to disk, if we have an actual sender.
      if (currentSender != nullptr) {
        WriteBufferInfo info;
        info.sender = currentSender;
        info.buffer = buffer;
        info.bytesToWrite = fullBlocks;
        writeQueue.enqueue(info);
      }

      // Make the new buffer the current buffer, which will release the old one back into the pool.
      buffer = newBuffer;
    }

    // If our server is running live, send all of the messages in the packet to the clients attached to
    // this camera.
    if ((status == OKAY) && (m_server->CurrentMode() == Storage_Module_Server::Mode::Live)) {
      uint32_t cameraID = receiver->m_ID;

      std::string ret = m_server->SendImageStreamPacketToClients(cameraID, packet);
      if (!ret.empty()) {
        if (m_verbosity >= 0) {
          std::cerr << "Storage_Module::StreamReceiverThread() error sending packet to clients: " << ret << std::endl;
        }
      }
    }
  }

  // Write the last partial buffer to disk if it has any data in it.  First zero-pad it to an even multiple
  // of the disk block size.
  if ((currentSender != nullptr) && (bytesInBuffer > 0)) {

    // Pad with zeroes to an even number of block sizes.
    while ((bytesInBuffer < buffer->size()) && (bytesInBuffer % m_persistentState.DiskBlockSize() != 0)) {
      (*buffer)[bytesInBuffer] = 0;
      bytesInBuffer++;
    }

    // Write
    WriteBufferInfo info;
    info.sender = currentSender;
    info.buffer = buffer;
    info.bytesToWrite = bytesInBuffer;
    writeQueue.enqueue(info);
  }

  // Wait for our queue to drain, then stop our sub-thread and wait for it to finish.
  while (writeQueue.awaitEmpty(0, std::chrono::milliseconds(10))) {}
  stop = true;
  writeThread.join();
}

Storage_Module::PersistentState::PersistentState(const std::string& filename)
  : PersistentState()
{
  // Store our file name for later use in loading and saving.
  m_fileName = filename;

  // If the file does not exist, create it with default values (initialized by the delegated constructor).
  if (!std::filesystem::exists(filename)) {
    json j;
    j["storingAtRestart"] = StoringAtRestart();
    j["diskBlockSize"] = DiskBlockSize();
    j["totalBufferSize"] = TotalBufferSize();
    j["highWaterMark"] = HighWaterMark();
    std::ofstream file(filename);
    if (!file.is_open()) {
      return;
    }
    file << j.dump(2);
    file.close();
  }

  // Read the file and set the values.  If we can't read one of them, keep the default value.
  LoadFromFile();
}

bool Storage_Module::PersistentState::LoadFromFile()
{
  // Read the file and set the values.  If we can't read one of them, keep the default value.
  std::ifstream file(m_fileName);
  if (!file.is_open()) {
    return false;
  }
  json j;
  file >> j;
  try {
    m_storingAtRestart = j["storingAtRestart"];
  } catch (...) {
    // Leave it alone.
  }
  try {
    m_diskBlockSize = j["diskBlockSize"];
  } catch (...) {
    // Leave it alone.
  }
  try {
    m_totalBufferSize = j["totalBufferSize"];
  } catch (...) {
    // Leave it alone.
  }
  try {
    m_highWaterMark = j["highWaterMark"];
  } catch (...) {
    // Leave it alone.
  }
  return true;
}

bool Storage_Module::PersistentState::SaveToFile() const
{
  // Write the file and set the values.
  json j;
  j["storingAtRestart"] = StoringAtRestart();
  j["diskBlockSize"] = DiskBlockSize();
  j["totalBufferSize"] = TotalBufferSize();
  j["highWaterMark"] = HighWaterMark();
  std::ofstream file(m_fileName);
  if (!file.is_open()) {
    return false;
  }
  file << j.dump(2);
  return true;
}
