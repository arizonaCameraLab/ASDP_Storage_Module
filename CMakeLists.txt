cmake_minimum_required(VERSION 3.16.0)
project(ASDP_Storage_Module LANGUAGES CXX)

#-----------------------------------------------------------------------------
# Set your favorite C++ standard for compiling the internal files.  The library
# will export things as extern "C" so this will not impact client programs.
# We need C++17 for the filesystem library.
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

#-----------------------------------------------------------------------------
if(WIN32)
    add_definitions(-DNOMINMAX)
endif(WIN32)

#-----------------------------------------------------------------------------
# Local CMake Modules
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

#-----------------------------------------------------------------------------
# Enable sorting projects within the solution on Visual Studio, to group
# Test and Example files together.
set_property(GLOBAL PROPERTY USE_FOLDERS ON)

#-----------------------------------------------------------------------------
# Checking for optional and required packages

find_package(ASDP_Core REQUIRED)
find_package(Doxygen)
find_package(OpenMP)
if(OPENMP_FOUND)
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${OpenMP_C_FLAGS}")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${OpenMP_CXX_FLAGS}")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${OpenMP_EXE_LINKER_FLAGS}")
endif()

#-----------------------------------------------------------------------------
# Build options.
option(BUILD_TESTS "Build test programs" ON)

if (DOXYGEN_FOUND)
    option(BUILD_DOXYGEN "Build DOxygen documentation" ON)
endif (DOXYGEN_FOUND)

if (BUILD_DOXYGEN)
    # set input and output files
    set(DOXYGEN_IN ${CMAKE_CURRENT_SOURCE_DIR}/docs/Doxyfile.in)
    set(DOXYGEN_OUT ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile)

    # request to configure the file
    configure_file(${DOXYGEN_IN} ${DOXYGEN_OUT} @ONLY)

    # note the option ALL which allows to build the docs together with the application
    add_custom_target( doc_doxygen ALL
        COMMAND ${DOXYGEN_EXECUTABLE} ${DOXYGEN_OUT}
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
        COMMENT "Generating API documentation with Doxygen"
        VERBATIM )
endif (BUILD_DOXYGEN)

#-----------------------------------------------------------------------------
# Build the libraries.

set (Storage_Module_INCLUDES
	Storage_Module.h
)
set (Storage_Module_SOURCES
	Storage_Module.cpp
)
add_library(Storage_Module STATIC ${Storage_Module_SOURCES} ${Storage_Module_HEADERS})
target_include_directories(Storage_Module PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
  $<INSTALL_INTERFACE:include>
)
target_link_libraries(Storage_Module ASDP_Core::ASDP_Core)

#-----------------------------------------------------------------------------
# Build the simulator.

set (ASDP_Storage_Module_HEADERS
)

set (ASDP_Storage_Module_SOURCES
	ASDP_Storage_Module.cpp
)

add_executable(ASDP_Storage_Module ${ASDP_Storage_Module_SOURCES} ${ASDP_Storage_Module_HEADERS})
target_link_libraries(ASDP_Storage_Module Storage_Module ASDP_Core::ASDP_Core)

install(TARGETS ASDP_Storage_Module EXPORT ${PROJECT_NAME}
  RUNTIME DESTINATION bin COMPONENT bin
  LIBRARY DESTINATION lib${LIB_SUFFIX} COMPONENT lib
  ARCHIVE DESTINATION lib${LIB_SUFFIX} COMPONENT lib
  INCLUDES DESTINATION include
  PUBLIC_HEADER DESTINATION include
)

#-----------------------------------------------------------------------------
# Build tests if we've been asked to.

if(BUILD_TESTS)

  enable_testing()

  set (CPP_TESTS
	  test_Storage_Module
  )
  foreach (BASE ${CPP_TESTS})
    set (APP ${BASE})
    add_executable (${APP} tests_cpp/${BASE}.cpp)
    set_target_properties(${APP} PROPERTIES FOLDER tests/C++)
    target_link_libraries(${APP} PRIVATE
	    Storage_Module
    )
    install(TARGETS ${APP} EXPORT ${PROJECT_NAME}
      RUNTIME DESTINATION bin
    )
    add_test(${APP} ${APP})
  endforeach (BASE)

endif(BUILD_TESTS)

