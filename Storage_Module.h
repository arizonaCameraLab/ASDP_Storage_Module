/*
 * Copyright (C) 2024: Arizona Board of Regents on Behalf of the University of Arizona
 */

#pragma once
#include <asdp_api.h>
#include <map>
#include <mutex>
#include <list>

namespace asdp {

/// @brief Per-serial-number server module that advertises on a specific port.
class Storage_Module_Server : public CoreServerBase {
  /// @brief Constructor
  /// @todo

protected:
  //=============================================================================
  // Override methods to implement the commands as needed.

  /// @brief Implement the specified command.
  /// @param command The command packet to implement.
  /// @param client The client that the command is coming from.
  void doReset(const CommandPacketReset&, ClientState& client) override;

  /// @brief Implement the specified command.
  /// @param command The command packet to implement.
  /// @param client The client that the command is coming from.
  void doConfigureTrigger(const CommandPacketConfigureTrigger&, ClientState& client) override;

  /// @brief Implement the specified command.
  /// @param command The command packet to implement.
  /// @param client The client that the command is coming from.
  void doSoftwareTrigger(const CommandPacketSoftwareTrigger&, ClientState& client) override;

  /// @brief Implement the specified command.
  /// @param command The command packet to implement.
  /// @param client The client that the command is coming from.
  void doStreamSubregion(const CommandPacketStreamSubregion&, ClientState& client) override;

  /// @brief Implement the specified command.
  /// @param command The command packet to implement.
  /// @param client The client that the command is coming from.
  void doCancelSubregion(const CommandPacketCancelSubregion&, ClientState& client) override;
};

/// @brief Storage module that acts as an intermediary between Core Modules and other clients.
class Storage_Module : public CoreClient {
public:
  /// @brief Constructor
  /// @param verbosity The verbosity level of the server, 0 for no verbosity, higher for more verbosity.
  Storage_Module(const std::string &NicNameIn, const std::string &NicNameOut,
    const std::string StorageRoot, int verbosity = 0);

  /// @brief Run continuously, processing commands from clients.
  /// @return Empty string on success, message describing the problem on failure.
  std::string run();

  /// @brief Test the class.
  /// @return Empty string on success, message describing the problem on failure.
  static std::string Test();

protected:

  // State variables

  /// One server per serial number found in the storage root directory.
  /// When we connect a client, we will also ensure that the server for that serial number is running.
  std::vector< std::shared_ptr<Storage_Module_Server> > m_servers;
};

} // namespace asdp
