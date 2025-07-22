/*
 * Copyright (C) 2024: Arizona Board of Regents on Behalf of the University of Arizona
 */

#pragma once

/**
* @file ElapsedTimeWithPause.h
* @brief Apache Strap-Down Pilotage utility class to maintain pausable elapsed time.
*
* @author ReliaSolve.
* @date June 24, 2024.
*/

#include <chrono>
#include <string>
#include <memory>

namespace asdp {

  /// @brief A class to measure elapsed time, with the ability to pause and resume the timer.
  class ElapsedTimeWithPause {
  public:
    /// @brief Constructor that resets the timer to zero.
    ElapsedTimeWithPause();

    /// @brief Pause the timer.
    void Pause();

    /// @brief Resume the timer.
    void Resume();

    /// @brief Reset the timer to zero.
    void Reset();

    /// @brief Report elapsed time in secconds, not counting time that was paused.
    /// @return Elapsed time in seconds, not counting time that was paused.
    double ElapsedTime() const;

    /// @brief Test the class.
    /// @return An empty string if the test passed, otherwise a message describing the failure.
    static std::string Test();

  protected:
    // Using a mutex (even a shared mutex) caused starvation of the unique_lock on Linux when
    // trying to pause and resume the timer by the multiple camera shared_lock calls for read
    // access because write locks are not prioritized on Linux in this case.  To deal with that
    // without resorting to Boost, we use a shared_ptr to a state object that holds the state of the timer.
    // This pointer is atomic so that we can safely read and write it from multiple threads without
    // needing to lock it. The resulting pointed-to state object provides a consistent state that does
    // not change while reading it, even though the m_state pointer itself may be replaced while a
    // function is still using the old state.

    typedef struct {
      std::chrono::time_point<std::chrono::steady_clock> start_time;
      std::chrono::duration<double> total_pause_time;
      bool is_paused;
      std::chrono::time_point<std::chrono::steady_clock> pause_start_time;
    } State;

    /// @brief Only access this variable using std::atomic_load and std::atomic_store.
    // This requires C++17 and later, which can handle calling these functions on the non-atomic shared_ptr type.
    std::shared_ptr<State> m_state;
  };

} // namespace asdp
