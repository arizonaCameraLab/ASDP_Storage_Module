/*
 * Copyright (C) 2024: Arizona Board of Regents on Behalf of the University of Arizona
 */

#pragma once
#include <chrono>
#include <string>

namespace asdp {

  class Elapsed_Time_With_Pause {
  public:
    /// @brief Constructor that resets the timer to zero.
    Elapsed_Time_With_Pause();

    /// @brief Pause the timer.
    void pause();

    /// @brief Resume the timer.
    void resume();

    /// @brief Reset the timer to zero.
    void reset();

    /// @brief Repor elapsed time in secconds, not counting time that was paused.
    /// @return Elapsed time in seconds, not counting time that was paused.
    double elapsed_time() const;

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
