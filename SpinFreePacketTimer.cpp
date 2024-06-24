/*
 * Copyright (C) 2024: Arizona Board of Regents on Behalf of the University of Arizona
 */

#pragma once

 /**
 * @file SpinFreePacketTimer.cpp
 * @brief Apache Strap-Down Pilotage utility class to provide spin-free stream-packet buffering.
 *
 * @author ReliaSolve.
 * @date June 24, 2024.
 */

#include "SpinFreePacketTimer.h"

using namespace asdp;

SpinFreePacketTimer::SpinFreePacketTimer(std::shared_ptr<asdp::ElapsedTimeWithPause> timer)
  : m_elapsedTimer(timer)
{
  // Spin up the thread.
  m_thread = std::thread(&SpinFreePacketTimer::WatchThread, this);
}

SpinFreePacketTimer::~SpinFreePacketTimer()
{
  // Spin down the thread.
  m_done = true;
  m_thread.join();
}

void SpinFreePacketTimer::DefineCameraQueues(uint32_t cameraId,
  std::shared_ptr< SpinFreeQueue< std::shared_ptr<PacketTime> > > inputQueue,
  std::shared_ptr< SpinFreeQueue< std::shared_ptr<asdp::StreamPacket> > > outputQueue)
{
  CameraQueues cameraQueues;
  cameraQueues.inputQueue = inputQueue;
  cameraQueues.outputQueue = outputQueue;

  std::lock_guard<std::mutex> lock(m_mutex);
  m_cameraQueues[cameraId] = cameraQueues;
  m_cameraKeys.insert(cameraId);
}

void SpinFreePacketTimer::RemoveCameraQueues(uint32_t cameraId)
{
  std::lock_guard<std::mutex> lock(m_mutex);
  m_cameraQueues.erase(cameraId);
  m_cameraKeys.erase(cameraId);
}

void SpinFreePacketTimer::WatchThread()
{
  // Make a map from camera ID to a PacketTime entry shared-pointer.
  // This will cache the packet times for each camera, pulling the first
  // entry off each queue and keeping it until it is ready to send.
  std::map<uint32_t, std::shared_ptr<PacketTime> > cameraPacketTimes;

  while (!m_done) {

    // Get the current set of camera keys, holding the lock for as short a time as possible.
    std::set<uint32_t> cameraKeys;
    {
      std::lock_guard<std::mutex> lock(m_mutex);
      cameraKeys = m_cameraKeys;
    }

    // For each camera, check the cached entry. If there is not one, try and get one from the input queue.
    // If it is time to send the packet, push it onto the output queue.
    for (uint32_t cameraId : cameraKeys) {

      // Get the cached packet time.
      std::shared_ptr<PacketTime> packetTime = cameraPacketTimes[cameraId];

      // If there is no cached packet time, try to get one from the input queue.
      if (!packetTime) {
        std::lock_guard<std::mutex> lock(m_mutex);
        std::shared_ptr< SpinFreeQueue< std::shared_ptr<PacketTime> > > inputQueue = m_cameraQueues[cameraId].inputQueue;
        if (inputQueue) {
          if (inputQueue->dequeue(packetTime, std::chrono::milliseconds(0))) {
            cameraPacketTimes[cameraId] = packetTime;
          }
        }
      }

      // If there is a cached packet time, check if it is time to send the packet.
      if (packetTime) {
        if (packetTime->elapsedTime <= m_elapsedTimer->ElapsedTime()) {
          std::lock_guard<std::mutex> lock(m_mutex);
          // Push the packet onto the output queue.
          std::shared_ptr< SpinFreeQueue< std::shared_ptr<asdp::StreamPacket> > > outputQueue =
            m_cameraQueues[cameraId].outputQueue;
          if (outputQueue) {
            // Send it and reset the cached packet time so that it will be re-filled next time.
            outputQueue->enqueue(packetTime->packet);
            cameraPacketTimes[cameraId].reset();
          }
        }
      }
    }
  }
}

std::string SpinFreePacketTimer::Test()
{
  // Construct an elapsed timer and a packet timer that uses it.
  std::shared_ptr<asdp::ElapsedTimeWithPause> et(new asdp::ElapsedTimeWithPause);
  SpinFreePacketTimer spt(et);

  // Define vectors of queues for the inputs and outputs to cameras.  Make the specified number of queues
  size_t numQueues = 10;
  std::vector<std::shared_ptr< SpinFreeQueue< std::shared_ptr<PacketTime> > > > inputQueues;
  std::vector<std::shared_ptr< SpinFreeQueue< std::shared_ptr<asdp::StreamPacket> > > > outputQueues;
  for (uint32_t i = 0; i < numQueues; i++) {
    inputQueues.push_back(std::make_shared< SpinFreeQueue< std::shared_ptr<PacketTime> > >());
    outputQueues.push_back(std::make_shared< SpinFreeQueue< std::shared_ptr<asdp::StreamPacket> > >());
  }

  // Add the queues to the packet timer.
  for (uint32_t i = 0; i < numQueues; i++) {
    spt.DefineCameraQueues(i, inputQueues[i], outputQueues[i]);
  }

  // Get a StreamPacket that we will use for all testing.  Do this by creating a StreamWriter
  // and getting its first packet.
  std::shared_ptr<asdp::SenderUDP> sr = std::make_shared<asdp::SenderUDP>("localhost", 10101);
  asdp::StreamWriter sw(sr);
  std::shared_ptr<asdp::StreamPacket> packet;
  if (OKAY != sw.GetCurrentPacket(packet)) {
    return "Failed to get a packet from the stream writer.";
  }

  // Add packets to each input queue with a 1-second, 2-second, and 3-second delay.
  for (uint32_t i = 0; i < numQueues; i++) {
    std::shared_ptr<PacketTime> pt1(new PacketTime);
    pt1->packet = packet;
    pt1->elapsedTime = 1.0;
    inputQueues[i]->enqueue(pt1);

    std::shared_ptr<PacketTime> pt2(new PacketTime);
    pt2->packet = packet;
    pt2->elapsedTime = 2.0;
    inputQueues[i]->enqueue(pt2);

    std::shared_ptr<PacketTime> pt3(new PacketTime);
    pt3->packet = packet;
    pt3->elapsedTime = 3.0;
    inputQueues[i]->enqueue(pt3);
  }

  // Verify that we cannot get a packet from the output queues before the time is up.
  for (uint32_t i = 0; i < numQueues; i++) {
    std::shared_ptr<asdp::StreamPacket> packet;
    if (outputQueues[i]->dequeue(packet, std::chrono::milliseconds(0))) {
      return "Got a packet from the output queue before it was ready.";
    }
  }

  // Wait a second and then make sure we can get the first packet from the output queues.
  // Do this three times.
  for (size_t s = 1; s <= 3; s++) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1001));
    for (uint32_t i = 0; i < numQueues; i++) {
      std::shared_ptr<asdp::StreamPacket> packet;
      if (!outputQueues[i]->dequeue(packet, std::chrono::milliseconds(0))) {
        return "Failed to get a packet from the output queue after it was ready after " + std::to_string(s) + " seconds.";
      }
    }
    for (uint32_t i = 0; i < numQueues; i++) {
      std::shared_ptr<asdp::StreamPacket> packet;
      if (outputQueues[i]->dequeue(packet, std::chrono::milliseconds(0))) {
        return "Got a packet from the output queue before it was ready after " + std::to_string(s) + " seconds.";
      }
    }
  }

  return "";
}
