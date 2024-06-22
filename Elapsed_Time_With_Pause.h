/*
 * Copyright (C) 2024: Arizona Board of Regents on Behalf of the University of Arizona
 */

#pragma once
#include <chrono>
#include <string>

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

    /// @brief Repor elapsed time in secconds, not counting time that was paused.
    /// @return Elapsed time in seconds, not counting time that was paused.
    double ElapsedTime() const;

    /// @brief Test the class.
    /// @return An empty string if the test passed, otherwise a message describing the failure.
    static std::string Test();

  protected:
    std::chrono::time_point<std::chrono::steady_clock> start_time;
    std::chrono::duration<double> total_pause_time;
    bool is_paused;
    std::chrono::time_point<std::chrono::steady_clock> pause_start_time;
  };

} // namespace asdp
