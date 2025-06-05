/*
 * Copyright (C) 2025: Arizona Board of Regents on Behalf of the University of Arizona
 */

// This program packs together multiple image (CONSOLIDATED_FRAME_DATA) messages into
// packets to reduce the number of packets sent over the network from a Storage Module.  It reads from a file containing
// these messages written by ASDP_Storage_Module and writes them to another file in the
// same format with fewer packets.  It ends a packet whenever a message has the end of frame flag set.

#include <string.h>
#include <iostream>
#include <cstdio>
#include <chrono>
#include <cstring>
#include <ASDP_Core_API.h>

using namespace asdp;

void usage(const std::string& programName)
{
  std::cerr << "Usage: " << programName << "<inFileName> <outFileName>" << std::endl;
  std::cerr << "  <inFileName> - The name of the file to parse." << std::endl;
  std::cerr << "  <outFileName> - The name of the file to write the packed messages to." << std::endl;
}

int main(int argc, char** argv)
{
  std::string inFileName, outFileName;
  size_t realParams = 0;

  // Parse the command line arguments, with the first non-flag argument being the
  // name of the file to parse.
  for (int i = 1; i < argc; ++i) {
    if (strcmp(argv[i], "--help") == 0) {
      usage(argv[0]);
      return 0;
    } else if (argv[i][0] == '-' ) {
      std::cerr << "Unknown flag: " << argv[i] << std::endl;
      return 1;
    } else switch (realParams++) {
      case 0:
        inFileName = argv[i];
        break;
      case 1:
        outFileName = argv[i];
        break;
      default:
        usage(argv[0]);
        return 1;
    }
  }
  if (realParams != 2) {
    usage(argv[0]);
    return 2;
  }

  // Open a file receiver to read the file.
  ReceiverFile receiver(inFileName);
  if (receiver.GetConstructorStatus() != OKAY) {
    std::cerr << "Error opening file " << inFileName << ": " << ErrorMessage(receiver.GetConstructorStatus()) << std::endl;
    return 3;
  }
  std::cout << "Reading file " << inFileName << std::endl;

  // Open a stream file and writer to write the packed messages to.
  std::shared_ptr<SenderFile> streamFile = std::make_shared<SenderFile>(outFileName);
  if (streamFile->GetConstructorStatus() != OKAY) {
    std::cerr << "Error opening file " << outFileName << ": " << ErrorMessage(streamFile->GetConstructorStatus()) << std::endl;
    return 4;
  }
  std::shared_ptr<StreamWriter> writer = std::make_shared<StreamWriter>(streamFile);
  if (writer->GetConstructorStatus() != OKAY) {
    std::cerr << "Error creating stream writer: " << ErrorMessage(writer->GetConstructorStatus()) << std::endl;
    return 5;
  }
  std::cout << "Writing packed messages to file " << outFileName << std::endl;

  // Get the first packet in the output file.
  std::shared_ptr<StreamPacket> outPacket;
  Status ret = writer->GetCurrentPacket(outPacket);
  if (ret != OKAY) {
    std::cerr << "Error getting current packet from writer: " << ErrorMessage(ret) << std::endl;
    return 6;
  }

  // Read and parse stream packets from the file, extracting consolidated frame data messages and acting on them.
  bool available = false;
  receiver.IsPacketAvailable(0.0, available);
  size_t badPackets = 0;
  size_t packetsRead = 0;
  size_t messagesRead = 0;
  size_t packetsWritten = 0;
  std::string imageFileName;
  std::vector<uint16_t> imageBuffer;
  while (available) {
    // Get the next packet.
    std::shared_ptr<StreamPacket> packet;
    size_t offset = 0;
    Status status = receiver.ReceiveStreamPacket(0.0, packet, offset);
    if (status == TIMEOUT) {
      break;
    }
    if (status != OKAY) {
      std::cerr << "Error reading packet: " << ErrorMessage(status) << std::endl;
      return 100;
    }
    packetsRead++;

    // Read and handle each frame message in the packet.
    std::shared_ptr<Message> msg;
    status = packet->GetNextMessage(msg);
    if (status != OKAY) {
      std::cerr << "Error reading message: " << ErrorMessage(status) << std::endl;
      uint32_t size;
      status = packet->GetTotalLength(size);
      if (status == OKAY) {
        std::cerr << "  Packet size: " << size << std::endl;
      } else {
        std::cerr << "  Error reading packet size: " << ErrorMessage(status) << std::endl;
      }
      badPackets++;
    }
    while (msg) {
      messagesRead++;
      MessageID type;
      status = msg->GetType(type);
      if (status != OKAY) {
        std::cerr << "Error reading message type: " << ErrorMessage(status) << std::endl;
        return 200;
      }
      Time time;
      status = msg->GetTime(time);
      if (status != OKAY) {
        std::cerr << "Error reading message time: " << ErrorMessage(status) << std::endl;
        return 201;
      }
      switch (type) {
        case CONSOLIDATED_FRAME_DATA:
          {
            // Get the consolidated frame data from the message.
            MessageConsolidatedFrameData frameData(*msg);
            if (frameData.GetConstructorStatus() != OKAY) {
              std::cerr << "Error reading consolidated frame data: " << ErrorMessage(frameData.GetConstructorStatus()) << std::endl;
              return 202;
            }

            // Add the message to the output packet.  If it fails, flush the writer and try again.
            ret = frameData.CopyToStreamPacket(*outPacket);
            if (ret != OKAY) {
              ret = writer->Flush();
              if (ret != OKAY) {
                std::cerr << "Error flushing writer: " << ErrorMessage(ret) << std::endl;
                return 203;
              }
              packetsWritten++;
              ret = writer->GetCurrentPacket(outPacket);
              if (ret != OKAY) {
                std::cerr << "Error getting current packet from writer after flush: " << ErrorMessage(ret) << std::endl;
                return 204;
              }
              ret = frameData.CopyToStreamPacket(*outPacket);
              if (ret != OKAY) {
                std::cerr << "Error copying consolidated frame data to flushed output packet: " << ErrorMessage(ret) << std::endl;
                return 205;
              }
            }

            // See if this holds the end of a frame. If so, we flush the writer once it has been
            // inserted.
            bool isEndFrame;
            status = frameData.GetEndFrameFlag(isEndFrame);
            if (status != OKAY) {
              std::cerr << "Error reading end frame: " << ErrorMessage(status) << std::endl;
              return 206;
            }
            if (isEndFrame) {
              ret = writer->Flush();
              if (ret != OKAY) {
                std::cerr << "Error flushing writer at end of frame: " << ErrorMessage(ret) << std::endl;
                return 207;
              }
              packetsWritten++;
              ret = writer->GetCurrentPacket(outPacket);
              if (ret != OKAY) {
                std::cerr << "Error getting current packet from writer after end-of-frame flush: " << ErrorMessage(ret) << std::endl;
                return 208;
              }
            }
          }
          break;

        default:
          // Other messages are ignored.
          break;
      }

      status = packet->GetNextMessage(msg);
      if (status != OKAY) {
        std::cerr << "Error reading message: " << ErrorMessage(status) << std::endl;
        uint32_t size;
        status = packet->GetTotalLength(size);
        if (status == OKAY) {
          std::cerr << "  Packet size: " << size << std::endl;
        } else {
          std::cerr << "  Error reading packet size: " << ErrorMessage(status) << std::endl;
        }
        badPackets++;
      }
    }

    available = false;
    receiver.IsPacketAvailable(0.0, available);
  }

  std::cout << "Read " << packetsRead << " packets and " << messagesRead << " messages." << std::endl;
  std::cout << "Wrote " << packetsWritten << " packets." << std::endl;

  // Flush the writer to ensure all packets are written out and then close the writer and file.
  ret = writer->Flush();
  if (ret != OKAY) {
    std::cerr << "Error flushing writer at end: " << ErrorMessage(ret) << std::endl;
    return 300;
  }
  writer.reset();
  streamFile.reset();

  if (badPackets) {
    std::cerr << "Error: " << badPackets << " packets had errors." << std::endl;
    return 1000;
  }

  return 0;
}
