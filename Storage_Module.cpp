/*
 * Copyright (C) 2024: Arizona Board of Regents on Behalf of the University of Arizona
 */

#include "Storage_Module.h"
#include <iostream>
#include <algorithm>
#include <limits>
#include <filesystem>

using namespace asdp;

Storage_Module_Server::Storage_Module_Server(Storage_Module* parent, uint32_t serialNumber, const std::string& NicName,
    uint16_t sendPort, uint16_t listenPort, uint32_t maxPayloadSize, int verbosity)
  : CoreServerBase(serialNumber, NicName, sendPort, listenPort, maxPayloadSize, verbosity)
  , m_parent(parent)
{
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

Storage_Module::Storage_Module(const std::string& NicNameIn, const std::string& NicNameOut,
                               const std::string StorageRoot, int verbosity)
  : CoreClient(NicNameIn)
  , m_status(CoreClient::GetConstructorStatus())
  , m_verbosity(verbosity)
  , m_nicNameIn(NicNameIn)
  , m_nicNameOut(NicNameOut)
  , m_storageRoot(StorageRoot)
  , m_stop(false)
  , m_nextPort(10101)
{
  if (m_status != OKAY) {
    return;
  }

  // Read the configuration file from the storage root directory.
  // If the file doesn't exist, create it with default values.
  /// @todo

  // Start a server thread for each serial number directory found in the storage root.
  // Use the factory function to determine the listening port for each server.
  for (const auto& entry : std::filesystem::directory_iterator(StorageRoot)) {
    if (entry.is_directory()) {
      // If we can't convert the name into an unsigned integer, skip it.
      uint32_t serialNumber = 0;
      try {
        serialNumber = std::stoul(entry.path().filename().string());
      } catch(...) {
        serialNumber = 0;
      }
      if (serialNumber > 0) {
        if (verbosity > 1) {
          std::cout << " Storage_Module::Starting server for serial# " << serialNumber << std::endl;
        }
        m_servers.emplace_back(
          std::make_shared<ServerInfo>(
          serialNumber,
          std::make_shared<Storage_Module_Server>(this, serialNumber, NicNameOut,
            10102, m_nextPort.fetch_sub(1), 9000 - 28, verbosity)));
        m_server_threads.emplace_back(std::thread(&Storage_Module::ServerThread, this, m_servers.back()));
      }
    }
  }

  // Start the client thread
  m_client_thread = std::thread(&Storage_Module::ClientThread, this);

  /// @todo

  if (m_verbosity > 0) {
    std::cout << "Storage_Module::Storage_Module() constructed" << std::endl;
  }
}

Storage_Module::~Storage_Module()
{
  // Stop all threads and wait for them to finish
  m_stop = false;

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
  // The Discovery thread is already running, so we don't need to start it here.
  // We watch for a server to show up in the discovery list, then we connect to it.
  while (!m_stop) {

    // If we are already connected to a server, check for and handle incoming data.
    if (m_stream != nullptr) {

      /// @todo

    // We are not connected to a server, so look for one in the list of identified servers.
    } else {
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
          // We're broken, so we can't do anything else.  Just set the status and return.
          m_status = status;
          return;
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

        // See if recording at start-up is enabled for this server.  If so, start recording.
        /// @todo

      } else {
        // Sleep briefly to keep from hogging the CPU while we're not connected.
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
      }
    }
  }
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
    // This should not exist.
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

  // Enable streaming for each of them.
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
        return UNEXPECTED_INTERNAL_STATE;
    }
  }

  // Get a list of the cameras that the server supports.
  std::vector<CameraInfo> cameras;
  status = response.GetCameras(cameras);

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
    std::shared_ptr<ReceiverInfo> receiverInfo = std::make_shared<ReceiverInfo>(stream);
    m_receiver_threads.emplace_back(std::thread(&Storage_Module::StreamReceiverThread, this, receiverInfo));

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

  /// @todo We may want two triggers when we have stereo cameras along with narrow-fields.
  // Configure the first trigger as a software trigger at the @todo rate and send a trigger.
  /// @todo

  // Configure all of the cameras to run from the first trigger.
  /// @todo


  /// @todo

  return OKAY;
}

void Storage_Module::StreamReceiverThread(std::shared_ptr<ReceiverInfo> receiver)
{
  if (m_verbosity > 2) {
    std::cout << "  Storage_Module::StreamReceiverThread() started" << std::endl;
  }
  while (!m_stop) {
    /// @todo
  }
}

std::string Storage_Module::Test()
{
  return "@todo implement Test()";
}
