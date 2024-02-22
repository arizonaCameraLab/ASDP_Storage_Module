/*
 * Copyright (C) 2024: Arizona Board of Regents on Behalf of the University of Arizona
 */

#include "Storage_Module.h"
#include <iostream>
#include <algorithm>
#include <limits>

using namespace asdp;

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
{
}
