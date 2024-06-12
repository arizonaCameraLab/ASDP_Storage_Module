/*
 * Copyright (C) 2024: Arizona Board of Regents on Behalf of the University of Arizona
 */

#include "Storage_Module.h"
#include <iostream>
#include <algorithm>
#include <limits>
#include <filesystem>
#include <thread>
#include <atomic>
#include <nlohmann/json.hpp>
#include <ASDP_BufferPool.h>
#include <ASDP_SpinFreeQueue.hpp>

using namespace asdp;
using json = nlohmann::json;

Storage_Module_Server::Storage_Module_Server(Storage_Module* parent, uint32_t serialNumber, const std::string& NicName,
    uint16_t sendPort, uint16_t listenPort, uint32_t maxPayloadSize, int verbosity)
  : CoreServerBase(serialNumber, NicName, sendPort, listenPort, maxPayloadSize, verbosity)
  , m_parent(parent)
{
  // Set our state to match the parent's state where appropriate.
  m_recordOnReset = m_parent->m_persistentState.StoringAtRestart();
  m_storing = m_parent->m_storageSenders.size() > 0;

  // Add the storage API to our features.
  m_features.push_back(STORAGE_API_AVAILABLE);

  /// @todo
}

void Storage_Module_Server::doEveryLoop()
{
  // If the threads are supposed to be stopping, set an error indicating
  // this.
  if (m_parent->m_stop) {
    m_error = "Server is stopping";
  }

  /// @todo
}

void Storage_Module_Server::doReset(const CommandPacketReset& command, ClientState& client)
{
  m_error = "@todo implement doReset";
}

void Storage_Module_Server::doConfigureTrigger(const CommandPacketConfigureTrigger& command, ClientState& client)
{
  m_error = "@todo implement doConfigureTrigger";
}

void Storage_Module_Server::doSoftwareTrigger(const CommandPacketSoftwareTrigger& command, ClientState& client)
{
  m_error = "@todo implement doSoftwareTrigger";
}

void Storage_Module_Server::doStreamSubregion(const CommandPacketStreamSubregion& command, ClientState& client)
{
  m_error = "@todo implement doStreamSubregion";
}

void Storage_Module_Server::doCancelSubregion(const CommandPacketCancelSubregion& command, ClientState& client)
{
  m_error = "@todo implement doCancelSubregion";
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

void Storage_Module_Server::doListStoredStreams(const CommandPacketListStoredStreams& command, ClientState& client)
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
    } catch(...) {
      // Nothing to do here.
    }
  }

  // Send the list of stored streams back to the client.
  Status status;
  /// @todo
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
      m_error = "doListStoredStreams(): Error constructing MessageStoredStreamList: " + ErrorMessage(message.GetConstructorStatus());
      return;
    }

    // Send the packet.
    status = client.m_writer->Flush();
    if (status != OKAY) {
      m_error = "doListStoredStreams(): Error flushing StreamWriter: " + ErrorMessage(status);
      return;
    }
  }
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
  if (m_client_thread.joinable()) {
    m_client_thread.join();
  }

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
        std::lock_guard<std::mutex> lock(m_storageMutex);

        // Drop the connection and we'll automatically try to reconnect later.
        m_stream.reset();
        m_server->m_storing = false;
        m_server.reset();
        m_storageSenders.clear();
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

        // If we have a server associated with this client, forward the message through it.
        // Squash any state message and do not pass it on.
        /// @todo

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
        // it supports.
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

  // Ensure that we are connected to a server before we start storing (we will have storage server pointers).
  if (m_storageSenders.size() == 0) {
    return OKAY;
  }

  // Create the appropriate directory to store our files into by finding the lowest unused ID (starting with 0)
  // that is available in the root directory under our serial number.
  uint32_t storageID = 0;
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
          << ErrorMessage(m_storageSenders[0]->GetConstructorStatus()) << std::endl;
        std::cerr << "  File name: " << fileName << std::endl;
      }
      return FILE_FAILURE;
    } else if (m_verbosity > 1) {
      std::cout << " Storage_Module::Opened storage file for camera stream: " << fileName << std::endl;
    }
  }

  // Set the flag that we are storing in our server.
  m_server->m_storing = true;

  return OKAY;
}

Status Storage_Module::StopStoring()
{
  std::lock_guard<std::mutex> lock(m_storageMutex);

  // Ensure that we are connected to a server before we start storing (we will have storage server pointers).
  if (m_storageSenders.size() == 0) {
    return OKAY;
  }

  // Close all the storage senders.
  for (auto sender : m_storageSenders) {
    sender.reset();
  }

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

  /// @todo Set the relevant state values in the server to those of the client we just connected to.

  return OKAY;
}

/// @brief Structure to hold data describing writing a buffer to disk.
struct WriteBufferInfo {
  std::shared_ptr<SenderFile> sender;
  std::shared_ptr<std::vector<uint8_t>> buffer;
  size_t bytesToWrite;
};

/// @brief Helper function to run as a thread that writes data to disk.
static void WriteBuffersToFile(asdp::SpinFreeQueue<WriteBufferInfo>& writeQueue, std::atomic<bool>& stop)
{
  WriteBufferInfo info;
  while (!stop) {
    if (writeQueue.dequeue(info, std::chrono::milliseconds(100))) {
      info.sender->Send(info.buffer->data(), info.bytesToWrite);
    }
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
    }

    // Get the next packet from the stream, adding it to the end of our existing buffer.
    // Time out after 1 ms so we can check for a stop condition.
    std::shared_ptr<StreamPacket> packet;
    Status status = receiver->m_receiver->ReceiveStreamPacket(1e-3, packet, bytesInBuffer, buffer);

    // See if we've reached the high water mark for the buffer.  If so, copy the remaining bytes
    // past the last full disk block size into a new buffer and then write the full-block-size portion
    // of the old buffer to disk.
    if (bytesInBuffer >= m_persistentState.DiskBlockSize()) {
      // Copy the remaining bytes into a new buffer.
      std::shared_ptr<std::vector<uint8_t>> newBuffer = bufferPool.GetBuffer();
      uint32_t bytesToCopy = m_persistentState.DiskBlockSize() * (bytesInBuffer / m_persistentState.DiskBlockSize());
      std::copy(buffer->data() + bytesToCopy, buffer->data() + bytesInBuffer, newBuffer->data());
      bytesInBuffer -= bytesToCopy;

      // Write the full block size to disk, if we have an actual sender.
      if (currentSender != nullptr) {
        WriteBufferInfo info;
        info.sender = currentSender;
        info.buffer = buffer;
        info.bytesToWrite = bytesToCopy;
        writeQueue.enqueue(info);
      }

      // Swap the new buffer into the old buffer.
      buffer = newBuffer;
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
    info.sender = m_storageSenders[receiver->m_ID];
    info.buffer = buffer;
    info.bytesToWrite = bytesInBuffer;
    writeQueue.enqueue(info);
  }

  // Wait for our queue to drain, then stop our sub-thread and wait for it to finish.
  while (!writeQueue.size()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  stop = true;
  writeThread.join();
}

std::string Storage_Module::Test()
{
  return "@todo implement Test()";
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
