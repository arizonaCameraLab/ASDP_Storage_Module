/*
 * Copyright (C) 2024: Arizona Board of Regents on Behalf of the University of Arizona
 */

#include <thread>
#include "ElapsedTimeWithPause.h"
using namespace asdp;

ElapsedTimeWithPause::ElapsedTimeWithPause()
{
  Reset();
}

void ElapsedTimeWithPause::Pause()
{
  std::lock_guard<std::mutex> lock(m_mutex);
  if (!is_paused) {
    is_paused = true;
    pause_start_time = std::chrono::steady_clock::now();
  }
}

void ElapsedTimeWithPause::Resume()
{
  std::lock_guard<std::mutex> lock(m_mutex);
  if (is_paused) {
    is_paused = false;
    total_pause_time += std::chrono::steady_clock::now() - pause_start_time;
  }
}

void ElapsedTimeWithPause::Reset()
{
  std::lock_guard<std::mutex> lock(m_mutex);
  start_time = std::chrono::steady_clock::now();
  total_pause_time = std::chrono::duration<double>::zero();
  is_paused = false;
}

double ElapsedTimeWithPause::ElapsedTime() const
{
  std::lock_guard<std::mutex> lock(m_mutex);
  std::chrono::time_point<std::chrono::steady_clock> now;
  if (is_paused) {
    now = pause_start_time;
  } else {
    now = std::chrono::steady_clock::now();
  }
  std::chrono::duration<double> elapsed_time = now - start_time - total_pause_time;
  return elapsed_time.count();
}

std::string ElapsedTimeWithPause::Test()
{
  // Construct an object of the class and make sure its initial time is near zero and it counts up
  // over time.
  ElapsedTimeWithPause et;
  double t0 = et.ElapsedTime();
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  double t1 = et.ElapsedTime();
  if (t0 > 1e-3) {
    return "Error: Initial elapsed time should be near zero.";
  }
  if (t1 <= t0) {
    return "Error: Elapsed time should be increasing.";
  }

  // Pause the timer and make sure it stops counting up.
  et.Pause();
  double t2 = et.ElapsedTime();
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  double t3 = et.ElapsedTime();
  if (t2 != t3) {
    return "Error: Elapsed time should not change when paused.";
  }

  // Resume the timer and make sure it starts counting up again.
  et.Resume();
  double t4 = et.ElapsedTime();
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  double t5 = et.ElapsedTime();
  if (t4 > t5) {
    return "Error: Elapsed time should be increasing after resuming.";
  }
  if (t4 < t3) {
    return "Error: Resume time should be at least as large as pause time.";
  }

  // Reset the timer and make sure it starts counting up from zero again.
  et.Reset();
  double t6 = et.ElapsedTime();
  if (t6 > 1e-3) {
    return "Error: Elapsed time should be near zero after resetting.";
  }

  // Everything worked.
  return "";
}