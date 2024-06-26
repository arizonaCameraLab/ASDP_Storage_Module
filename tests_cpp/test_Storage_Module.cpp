/*
 * Copyright (C) 2024: Arizona Board of Regents on Behalf of the University of Arizona
 */

#include <iostream>
#include "ElapsedTimeWithPause.h"
#include "SpinFreePacketTimer.h"

int main(int argc, char** argv)
{
  // Test classes that we depend on.
  std::string res = asdp::ElapsedTimeWithPause::Test();
  if (res != "") {
    std::cerr << "Elapsed_Time_With_Pause test failed: " + res << std::endl;
    return 1;
  }

  res = asdp::SpinFreePacketTimer::Test();
  if (res != "") {
    std::cerr << "Storage_Module::Test(): SpinFreePacketTimer test failed: " + res << std::endl;
  }

  std::cout << "Run Storage_Validating_Client from ASDP_Core_API repository to further test." << std::endl;

  return 0;
}
