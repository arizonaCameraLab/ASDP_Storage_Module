/*
 * Copyright (C) 2024: Arizona Board of Regents on Behalf of the University of Arizona
 */

#pragma once

 /**
 * @file SpinFreePacketTimer.h
 * @brief Apache Strap-Down Pilotage utility class to provide spin-free stream-packet buffering.
 *
 * @author ReliaSolve.
 * @date June 24, 2024.
 */

#include <chrono>
#include <memory>
#include <string>
#include <map>
#include <set>
#include <cstdint>
#include <thread>
#include <mutex>
#include <atomic>
#include <ASDP_SpinFreeQueue.hpp>
#include <ASDP_Core_API.h>
#include "ElapsedTimeWithPause.h"

namespace asdp {

  /// @brief A class to queue stream packets from replay until they are ready to send.
  /// @details This class provides a spin-free way to buffer stream packets from replay until they are ready to send.
  /// The class uses an elapsed timer to determine when packets are ready to send. The class is thread-safe.
  /// It does have one thread that spin-waits on all input queues, but does not require each waiting thread to spin.
  /// @todo Spin the queue up and down as needed.
  class SpinFreePacketTimer {
  public:
    /// @brief Constructor.
    /// @param timer The elapsed timer to watch for packets that are ready to send.
    SpinFreePacketTimer(std::shared_ptr<asdp::ElapsedTimeWithPause> timer);

    /// @brief Destructor.
    ~SpinFreePacketTimer();

    /// @brief Structure holiding a packet and the time it is ready to send.
    struct PacketTime {
      std::shared_ptr<asdp::StreamPacket> packet; ///< The packet.
      double elapsedTime = 0; ///< The time the packet is ready to send since stream start.
    };

    /// @brief Define the input and output queues for a camera, overwriting any existing entry.
    /// @param cameraId The camera ID.
    /// @param inputQueue The input queue.
    /// @param outputQueue The output queue.
    void DefineCameraQueues(uint32_t cameraId,
      std::shared_ptr< SpinFreeQueue< std::shared_ptr<PacketTime> > > inputQueue,
      std::shared_ptr< SpinFreeQueue< std::shared_ptr<asdp::StreamPacket> > > outputQueue);

    /// @brief Remove the input and output queues for a camera, if there is one.
    /// @param cameraId The camera ID.
    /// @return True if the camera queues were removed, otherwise false.
    void RemoveCameraQueues(uint32_t cameraId);

    /// @brief Test the class.
    /// @return An empty string if the test passed, otherwise a message describing the failure.
    static std::string Test();

  protected:
    /// @brief Structure with the input queue and output queue for a given camera.
    struct CameraQueues {
      std::shared_ptr< SpinFreeQueue< std::shared_ptr<PacketTime> > > inputQueue; ///< The input queue.
      std::shared_ptr< SpinFreeQueue< std::shared_ptr<asdp::StreamPacket> > > outputQueue; ///< The output queue.
    };

    /// @brief The map of camera queues.
    std::map<uint32_t, CameraQueues> m_cameraQueues;

    /// @brief Set of keys in the map.
    std::set<uint32_t> m_cameraKeys;

    /// @brief The elapsed timer to watch for packets that are ready to send.
    std::shared_ptr<asdp::ElapsedTimeWithPause> m_elapsedTimer;

    /// @brief Thread to watch for packets that are ready to send from any input queue.
    std::thread m_thread;
    void WatchThread();

    /// @brief Mutex to protect the camera queues.
    std::mutex m_mutex;

    /// @brief Flag to indicate the watch thread should stop.
    std::atomic_bool m_done;
  };

} // namespace asdp
