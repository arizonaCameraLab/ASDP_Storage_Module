/*
 * Copyright (C) 2024: Arizona Board of Regents on Behalf of the University of Arizona
 */

#include <thread>
#include <atomic>
#include "ElapsedTimeWithPause.h"
using namespace asdp;

ElapsedTimeWithPause::ElapsedTimeWithPause()
{
  Reset();
}

void ElapsedTimeWithPause::Pause()
{
  std::shared_ptr<State> oldState = std::atomic_load(&m_state);
  std::shared_ptr<State> newState = std::make_shared<State>();
  *newState = *oldState; // Make a copy of the current state.
  if (!newState->is_paused) {
    newState->is_paused = true;
    newState->pause_start_time = std::chrono::steady_clock::now();
  }
  std::atomic_store(&m_state, newState);
}

void ElapsedTimeWithPause::Resume()
{
  std::shared_ptr<State> oldState = std::atomic_load(&m_state);
  std::shared_ptr<State> newState = std::make_shared<State>();
  *newState = *oldState; // Make a copy of the current state.
  if (newState->is_paused) {
    newState->is_paused = false;
    newState->total_pause_time += std::chrono::steady_clock::now() - newState->pause_start_time;
  }
  std::atomic_store(&m_state, newState);
}

void ElapsedTimeWithPause::Reset()
{
  std::shared_ptr<State> newState = std::make_shared<State>();
  newState->start_time = std::chrono::steady_clock::now();
  newState->total_pause_time = std::chrono::duration<double>::zero();
  newState->is_paused = false;
  std::atomic_store(&m_state, newState);
}

double ElapsedTimeWithPause::ElapsedTime() const
{
  // Make a local copy of the state to avoid having its contents be changed non-atomically by another thread.
  std::shared_ptr<State> local_state = std::atomic_load(&m_state);
  std::chrono::time_point<std::chrono::steady_clock> now;
  if (local_state->is_paused) {
    now = local_state->pause_start_time;
  } else {
    now = std::chrono::steady_clock::now();
  }
  std::chrono::duration<double> elapsed_time = now - local_state->start_time - local_state->total_pause_time;
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
