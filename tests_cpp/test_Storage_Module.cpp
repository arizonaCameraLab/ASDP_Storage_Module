/*
 * Copyright (C) 2024: Arizona Board of Regents on Behalf of the University of Arizona
 */

#include <iostream>
#include <Storage_Module.h>

int main(int argc, char** argv)
{
  std::string ret = asdp::Storage_Module::Test();
  if (ret.size() > 0) {
    std::cerr << "Error: " << ret << std::endl;
    return 1;
  }
  std::cout << "Success" << std::endl;
  return 0;
}
