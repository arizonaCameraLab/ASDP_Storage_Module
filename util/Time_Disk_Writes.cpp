#include <iostream>
#include <fstream>
#include <atomic>
#include <mutex>
#include <list>
#include <memory>
#include <vector>
#include <chrono>
#include <thread>

/// @brief Thread function to write data to a file.
/// @param fileName The name of the file to write to.
/// @param stopFlag A flag to signal the thread to stop.
/// @param fps The frames/second written to the file.
/// @note This function will write data to the file until the stop flag is set.
void writeToFile(const std::string& fileName, std::atomic<bool>& stopFlag, double &fps)
{
  std::ofstream outFile(fileName, std::ios::binary);
  if (!outFile.is_open()) {
    std::cerr << "Error: Failed to open file " << fileName << std::endl;
    fps = -1;
    return;
  }
  std::vector<unsigned char> data(1280*1024*2);

  auto startTime = std::chrono::steady_clock::now();

  size_t count = 0;
  while (!stopFlag) {
    outFile.write(reinterpret_cast<const char*>(data.data()), data.size());
    ++count;
  }

  outFile.close();

  auto endTime = std::chrono::steady_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::seconds>(endTime - startTime);
  fps = static_cast<double>(count) / duration.count();
}

int main(int argc, char* argv[]) {
  if (argc < 4) {
    std::cerr << "Usage: " << argv[0] << " <time_in_seconds> <number_of_cameras> <directories...>\n";
    std::cerr << "  time_in_seconds: The number of seconds to write data to the files.\n";
    std::cerr << "  number_of_cameras: The number of camera files to write to.\n";
    std::cerr << "  directories: One or more directories to write the files to. Use /dev/null to write all files to /dev/null\n";
    std::cerr << std::endl;
    std::cerr << "This program opens as many threads as there are cameras and writes data to the files\n";
    std::cerr << "in the specified directories. The program will write data for the specified number of\n";
    std::cerr << "seconds and then exit, reporting the frames/second written by each camera.\n";
    std::cerr << "The files are written round-robin to the specified directories.\n";
    std::cerr << std::endl;
    return 1;
  }

  int timeInSeconds = std::stoi(argv[1]);
  int numberOfCameras = std::stoi(argv[2]);
  std::vector<std::string> directories;
  for (int i = 3; i < argc; ++i) {
    directories.push_back(argv[i]);
  }

  std::vector<double> fps(numberOfCameras, 0.0);

  std::atomic<bool> stopFlag(false);

  // Start threads to write data to the files
  std::cout << "Writing to " << numberOfCameras << " cameras for " << timeInSeconds << " seconds" << std::endl;
  std::vector<std::thread> threads;
  for (int i = 0; i < numberOfCameras; ++i) {
    std::string directory = directories[i % directories.size()];
    std::string fileName = directory + "/" + std::to_string(i + 1) + ".asdp";
    if ((directory == "/dev/null") || (directory == "NUL:")) {
      fileName = directory;
    }
    threads.push_back(std::thread(writeToFile, fileName, std::ref(stopFlag), std::ref(fps[i])));
  }

  // Wait for the threads to start
  std::this_thread::sleep_for(std::chrono::seconds(1));

  // Continue writing data to the files until the time has elapsed
  auto startTime = std::chrono::steady_clock::now();
  while (std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - startTime).count() < timeInSeconds) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  stopFlag = true;

  for (auto& thread : threads) {
    if (thread.joinable()) {
      thread.join();
    }
  }

  for (size_t i = 0; i < fps.size(); i++) {
    std::cout << "FPS for " << i << ": " << fps[i] << std::endl;
  }

  return 0;
}
