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
        // Lock the mutex to keep state from changing while we're working.
        std::lock_guard<std::recursive_mutex> lock(m_mutex);

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

void Storage_Module_Server::doEveryLoop()
{
  // If the threads are supposed to be stopping, set an error indicating
  // this.
  if (m_parent->m_stop) {
    m_error = "Server is stopping";
  }

  /// @todo
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

        // Lock the mutex to keep state from changing while we're working.
        std::lock_guard<std::recursive_mutex> lock(m_mutex);

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

        // Get the information about the server we just connected to.
        /// @todo

        // See if recording at start-up is enabled for this server.  If so, start recording.
        /// @todo

        // Establish the receiver threads for this server that will store and perhaps forward the data.
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
  // Lock the mutex to keep state from changing while we're working.
  std::lock_guard<std::recursive_mutex> lock(m_mutex);

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

std::string Storage_Module::Test()
{
  return "@todo implement Test()";
}
