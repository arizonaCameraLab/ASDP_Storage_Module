/*
 * Copyright (C) 2024: Arizona Board of Regents on Behalf of the University of Arizona
 */

// This is a storage module that (optionally) listens for commands from a client and
// (optionally) acts as a Core_Module to other clients.

#include <cstdlib>
#include <iostream>
#include <chrono>
#include <asdp_api.h>
#include <Storage_Module.h>

using namespace asdp;

void Usage(const char* programName, int code)
{
  std::cerr << "Usage: " << programName << " [--verbosity V] NICNameIn NICNameOut StorageRoot" << std::endl;
  std::cerr << "  --verbosity V: Set the verbosity level to V (default 0)" << std::endl;
  std::cerr << "  NICNameIn: The IP address or DNS name of the NIC to listen on" << std::endl;
  std::cerr << "  NICNameOut: The IP address or DNS name of the NIC to broadcast on" << std::endl;
  std::cerr << "  StorageRoot: Root directory of the storage, which has a directory for each serial number" << std::endl;
  std::exit(code);
}

int main(int argc, char** argv)
{
  std::string NICNameIn, NICNameOut, StorageRoot;
  size_t realParams = 0;
  int verbosity = 0;

  // Parse the command line arguments, with the first non-flag argument being the
  // name of the IP address to broadcast on.  There is a --serial flag to specify
  // the serial number of the server, which defaults to 1.
  for (int i = 1; i < argc; ++i) {
  if (std::string(argv[i]) == "--verbosity") {
    if (i + 1 < argc) {
        verbosity = std::stoi(argv[i + 1]);
        ++i;
      } else {
        std::cerr << "--verbosity flag requires an argument" << std::endl;
        return 1;
      }
    } else if (argv[i][0] == '-' ) {
      std::cerr << "Unknown flag: " << argv[i] << std::endl;
      return 1;
    } else switch (realParams++) {
      case 0:
        NICNameIn = argv[i];
        break;
      case 1:
        NICNameOut = argv[i];
        break;
      case 2:
        StorageRoot = argv[i];
        break;
      default:
        Usage(argv[0], 2);
    }
  }
  if (realParams != 3) {
    Usage(argv[0], 2);
  }
  if (NICNameIn == NICNameOut) {
    std::cerr << "NICNameIn and NICNameOut must be different" << std::endl;
    return 2;
  }

  // Open a Storage Module, specifying the NIC to listen and broadcast on.
  std::cout << "Opening storage module listening on " << NICNameIn << " and broadcasting on " << NICNameOut << std::endl;
  Storage_Module sModule(NICNameIn, NICNameOut, StorageRoot);
  if (sModule.GetConstructorStatus() != OKAY) {
    std::cerr << "Failed to open module: " << ErrorMessage(sModule.GetConstructorStatus()) << std::endl;
    return 3;
  }

  // Start the module.  Print the error message if it fails.
  std::cerr << "Module failed: " << sModule.run() << std::endl;

  return 0;
}
