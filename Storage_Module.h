/*
 * Copyright (C) 2024: Arizona Board of Regents on Behalf of the University of Arizona
 */

#pragma once

 /**
 * @file Storage_Module.h
 * @brief Apache Strap-Down Pilotage classes to implement a Storage (and replay) Module.
 *
 * @author ReliaSolve.
 * @date 2024.
 */

#include "ElapsedTimeWithPause.h"
#include <ASDP_Core_API.h>
#include <ASDP_SpinFreeQueue.hpp>
#include <map>
#include <mutex>
#include <shared_mutex>
#include <list>
#include <atomic>

namespace asdp {

class Storage_Module;

/// @brief Per-serial-number server module that advertises on a specific port.
class Storage_Module_Server : public CoreServerBase {
public:

  /// @brief Constructor
  /// @param parent The parent object that created this server. Note: This is an unprotected pointer.
  /// We must be careful to ensure that the parent object outlives this object. This is being done by
  /// waiting for all threads to stop before deleting the parent object.
  /// @param serialNumber The serial number of the server.
  /// @param NicName The name of the network interface to listen on for incoming connections.
  /// @param sendPort The port to send outgoing connections on.
  /// @param listenPort The port to listen on for incoming connections.
  /// @param maxPayloadSize The maximum size of a payload that can be sent or received.
  /// @param verbosity The verbosity level of the server, 0 for no verbosity, higher for more verbosity.
  Storage_Module_Server(Storage_Module *parent, uint32_t serialNumber, const std::string &NicName,
    uint16_t sendPort, uint16_t listenPort, uint32_t maxPayloadSize, int verbosity);

  /// @brief Destructor
  /// Tears down all threads, closing all connections.
  ~Storage_Module_Server();

protected:

  /// The parent object that created this server.
  Storage_Module *m_parent;

  /// @brief Handle operations that must be done every loop, like checking if our thread should stop.
  void doEveryLoop() override;
  std::chrono::steady_clock::time_point m_lastCheckDiskSpace{};
  std::chrono::steady_clock::time_point m_lastEveryIteration{};
  std::chrono::steady_clock::time_point m_lastReportIterationTime{};
  double m_maxTimePerIteration = 0.0;
  double m_meanTimePerIteration = 0.0;
  unsigned m_iterationCount = 0;

  /// @brief Handle a client being closed.
  void clientBeingRemoved(ClientState& client) override;

  /// @brief Copy of state message that we will modify and send out when we are in idle mode.
  std::shared_ptr<MessageState> m_stateMessage;

  /// @brief Configure our internal state based on the current stored state message.
  /// @return Status indicating success or failure.
  Status ConfigureStateFromStoredState();

  /// @brief Read our initial time and message state from the specified file.
  /// @param [in] streamID The ID of the session to read the initial time and state from.
  /// @param [out] stateMessage The message state that was read.
  /// @return Empty string on success, message describing the problem on failure.
  std::string ReadInitialTimeAndState(uint32_t streamID, std::shared_ptr<MessageState> &stateMessage);


  /// @brief Adjust the specified stream-replay time to match the replay time base.
  /// @param timeCode The time code to adjust.
  Time AdjustTimeForReplay(Time const &timeCode) const;

  /// @brief Get a vector of stored stream IDs.
  /// @return A vector of stored stream IDs.
  std::vector<uint32_t> getStoredStreamIDs() const;

  /// @brief Send a state message with a different time and new storage information.
  /// @details This replaces the original time of the message, ensures that the storage feature
  /// is enabled, and sets the storage information based on our current status.
  /// @param original The original state message.
  /// @param timeCode The time code to use in the state message.  If this is zero, the time code
  /// from the original message will be used.
  /// @param client The client to send the message to.
  /// @return Status indicating success or failure.
  Status SendModifiedStateMessage(std::shared_ptr<MessageState> original, Time timeCode, ClientState& client);

  /// @brief Send all non-squelched messages in a packet to all clients, adjusting state as needed.
  /// @param [in] packet The packet to send the messages from.
  /// @param [in] adjustTime True if we should adjust the time of the messages based on an offset between
  /// their original time and our current replay time.  When false, the original time of the messages will be used.
  /// This is expected to be true for replaying and false for live streaming.
  /// @return Empty string on success, message describing the problem on failure.
  std::string ForwardPacketToClients(std::shared_ptr<StreamPacket> packet, bool adjustTime);

  //=============================================================================
  // Mode of operation.

  /// @brief Mode of operation
  enum class Mode {
    /// We are neither live nor replaying.
    Idle,
    /// We are live.
    Live,
    /// We are replaying.
    Replaying
  };

  /// @brief Return the current mode of operation.
  Mode CurrentMode() const;

  //=============================================================================
  // Replay-related state and methods.

  std::atomic_bool m_replayPaused;  ///< True if we are paused, false if we are playing.
  /// The time sent as part of the most-recent start-replay command.  Note that there is
  /// not one of these per client, but one for the server as a whole.  If any client asks
  /// for replay at a specific time, they all see replay at that time.
  Time m_replayInitialTime;
  Time m_replayFirstTime;           ///< The time of the first message in the replay file.
  std::shared_ptr<asdp::ElapsedTimeWithPause> m_replayElapsedTime;  ///< The elapsed time of the replay.
  /// The files we are replaying from. The 0th is the main stream and the rest are image streams.
  std::vector< std::shared_ptr<ReceiverFile> > m_replayFiles;
  std::shared_mutex m_replayMutex;         ///< Mutex to protect the replay state, including read and write locks.
  /// The next packet we are currently waiting to replay, which may be in the future. We use this
  /// to look ahead one packet to see if we should pause.
  std::shared_ptr<StreamPacket> m_replayPacket;
  Time m_replayPacketTime;          ///< The time of the next packet we are currently waiting to replay.
  /// @brief Get the current replay time.
  /// @details We make this into a function so that multiple threads don't need to rely on the
  /// main thread to update m_streamReplayTime in a continuous fashion, but can directly determine the
  /// time they should be replaying to.
  /// @return The current replay time.
  Time getCurrentReplayTime() const;

  /// @brief Structure holiding a packet and the time it is ready to send.
  struct PacketTime {
    std::shared_ptr<asdp::StreamPacket> packet; ///< The packet.
    double elapsedTime = 0.0; ///< The time the packet is ready to send since stream start.
  };

  // @brief Description of a camera to be handled by the replay thread.
  struct ReplayCameraDescription {
    uint32_t cameraID = 0;  ///< The ID of the camera, initially invalid.
    std::shared_ptr<ReceiverFile> receiver;  ///< The file to read packets from.
  };

  /// @brief Body of a thread that replays for a vector of cameras.
  /// @details To reduce the overall system load, each thread handles multiple cameras.
  /// For each camera, it reads packets from the file for each camera and sends them to all clients
  /// associated with each camera when their time has arrived.
  /// @param cameras Vector of descriptions of cameras to handle.
  void ReplayThread(std::vector<ReplayCameraDescription> cameras);

  /// Tells the replay per-camera streams to stop.
  std::atomic_bool m_stopReplayThreads;
  /// Replay threads for each camera.
  std::vector< std::thread > m_replayThreads;

  /// @brief Body of a thread that reads packets from disk and queues them for a single camera.
  /// @param cameraID Camera ID this is supporting (used for debug print info)
  /// @param receiver The file to read packets from.
  /// @param inputQueue The queue to send packets to.
  void ReplayInputThread(unsigned cameraID, std::shared_ptr<ReceiverFile> receiver,
    std::shared_ptr< SpinFreeQueue< std::shared_ptr<PacketTime> > > inputQueue);

  /// @brief Send all messages from the image packet to relevant clients.
  /// @param cameraID The ID of the camera the packet is streaming from.
  /// @param packet The packet to send the messages from.
  /// @param subtractTime The time to subtract from the time of each message, clamping to zero.
  /// @param addTime The time to add to the time of each message after subtracting the first.
  /// @return Empty string on success, message describing the problem on failure.
  std::string SendImageStreamPacketToClients(uint32_t cameraID, std::shared_ptr<StreamPacket> packet,
    Time subtractTime = Time(), Time addTime = Time());

  /// Structure to hold the information needed to replay to a single camera.
  struct ReplayInfo {
    SubregionDescription subregion = {};  ///< The subregion to replay to.
    std::shared_ptr<StreamWriter> writer;  ///< The writer to write packets using.
    /// Keeps track of where we are in our sending behavior.  If we have not yet found a begin-frame
    /// message that is after the requested start time, this will be -1.  Once we have found one,
    /// it will count up from 0 at each begin-frame message.  When this is positive, we're skipping
    /// messages until we reach the desired frame.  Once it reaches the count modulo the skipping
    /// interval, we will send the message.  If the skipping interval is 0, we will send every message.
    int state = -1;
  };
  /// Map from camera ID to a map from client to a map from endpoint to subregion description.
  /// Empty if we are not streaming subregions on this camera.
  std::map<uint32_t,
    std::map<ClientState,
      std::map<StreamEndpoint, std::shared_ptr<ReplayInfo> > > > m_subregions;
  std::shared_mutex m_subregionMutex;         ///< Mutex to protect m_subregions.

  //=============================================================================
  // Override methods to send messages generated by the base class so we can control when they
  // happen and what they send.
  Status SendStateMessage(ClientState& client) override;
  Status SendClockSyncMessage(ClientState& client) override;

  //=============================================================================
  /// Override methods to implement the commands as needed.

  /// Event verbosity is properly handled in our parent class.

  /// Set state period and other base-class ones should also forward to the live server if there is one
  void doReset(const CommandPacketReset&, ClientState& client) override;
  void doSetStreamStatePeriod(const CommandPacketSetStreamStatePeriod& command, ClientState& client) override;
  void doSetNUCFlagState(const CommandPacketSetNUCFlagState& command, ClientState& client) override;
  void doStartOnCameraNUC(const CommandPacketStartOnCameraNUC& command, ClientState& client) override;
  void doConfigureTrigger(const CommandPacketConfigureTrigger&, ClientState& client) override;
  void doSoftwareTrigger(const CommandPacketSoftwareTrigger&, ClientState& client) override;
  void doStreamSubregion(const CommandPacketStreamSubregion&, ClientState& client) override;
  void doCancelSubregion(const CommandPacketCancelSubregion&, ClientState& client) override;


  /// Override recording and replay methods.
  void doStartRecording(const CommandPacketStartRecording& command, ClientState& client) override;
  void doStopRecording(const CommandPacketStopRecording& command, ClientState& client) override;
  void doSetStartUpRecordingState(const CommandPacketSetStartUpRecordingState& command, ClientState& client) override;
  void doListStoredStreams(const CommandPacketListStoredStreams& command, ClientState& client) override;
  void doEraseStoredStream(const CommandPacketEraseStoredStream& command, ClientState& client) override;
  void doEraseAllStoredStreams(const CommandPacketEraseAllStoredStreams& command, ClientState& client) override;
  void doStartReplay(const CommandPacketStartReplay& command, ClientState& client) override;
  void doPauseReplay(const CommandPacketPauseReplay& command, ClientState& client) override;
  void doResumeReplay(const CommandPacketResumeReplay& command, ClientState& client) override;
  void doStopReplay(const CommandPacketStopReplay& command, ClientState& client) override;

  friend class Storage_Module;
};

/// @brief Storage module that acts as an intermediary between Core Modules and other clients.
class Storage_Module : public CoreClient {
public:
  /// @brief Constructor
  /// 
  /// This constructor will create a server for each serial number found in the storage root directory,
  /// along with a thread to run each server.  It also creates a client to connect to the Core Module,
  /// also in its own thread.  It then accepts any connections on the servers and forwards them to the
  /// Core Module, and accepts any responses from the Core Module and forwards them to the clients if
  /// they are using the same serial number.  It updates the CurrentStatus() to indicate whether the
  /// object is doing okay.
  /// @param NicNameIn The name of the network interface to listen on for incoming connections.
  /// @param NicNameOut The name of the network interface to send outgoing connections on.
  /// @param StorageRoot The root directory for storage.
  /// @param verbosity The verbosity level of the server, 0 for no verbosity, higher for more verbosity.
  /// A negative verbosity will cause the server not to report error messages to the console.
  Storage_Module(const std::string &NicNameIn, const std::string &NicNameOut,
    const std::string StorageRoot, int verbosity = 0);

  /// @brief Destructor
  ///
  /// Tears down all threads, closing all connections.
  ~Storage_Module();

  /// @brief Get the current status of the object.
  Status GetCurrentStatus() const;

protected:

  /// The current status of the object.
  Status m_status;

  /// The verbosity level of the server, 0 for no verbosity, higher for more verbosity.
  int m_verbosity;

  /// The name of the network interface to listen on for incoming connections.
  std::string m_nicNameIn;

  /// The name of the network interface to listen on for outgoing connections.
  std::string m_nicNameOut;

  /// The serial number of the device we are connected to.
  uint32_t m_serial;

  //=============================================================================
  /// Persistent state that is stored to disk and loaded from disk.

  class PersistentState {
  public:
    /// @brief Constructor
    /// @param fileName The name of a file to read the state from.  If the file does not
    /// exist, the state will be initialized to default values and then written to the file.
    /// If the file cannot be created, the state will still be initialized to default values.
    PersistentState(std::string const &fileName);

    /// @brief Load the state from disk.
    /// @return Status indicating success or failure.
    bool LoadFromFile();

    /// @brief Save the state to disk.
    /// @return Status indicating success or failure.
    bool SaveToFile() const;

    /// @brief Get the major version of the API stored in the files in this directory.
    uint16_t MajorVersion() const { return m_majorVersion; }

    /// @brief Get the minor version of the API stored in the files in this directory.
    uint16_t MinorVersion() const { return m_minorVersion; }

    /// @brief Get the patch version of the API stored in the files in this directory.
    uint16_t PatchVersion() const { return m_patchVersion; }

    void SetVersion(uint16_t major, uint16_t minor, uint16_t patch) {
      m_majorVersion = major;
      m_minorVersion = minor;
      m_patchVersion = patch;
    }

    /// @brief Find out whether we are storing to disk at restart.
    bool StoringAtRestart() const { return m_storingAtRestart; }

    /// @brief Set whether we are storing to disk at restart.
    /// @param storingAtRestart True if we are storing to disk at restart.
    void SetStoringAtRestart(bool storingAtRestart) { m_storingAtRestart = storingAtRestart; }

    /// @brief Get the number of bytes in a disk block.
    uint32_t DiskBlockSize() const { return m_diskBlockSize; }

    /// @brief Set the number of bytes in a disk block.
    /// @param diskBlockSize The number of bytes in a disk block. Ensures that we write to disk in multiples of this size.
    void SetDiskBlockSize(uint32_t diskBlockSize) { m_diskBlockSize = diskBlockSize; }

    /// @brief Get the total buffer size for each UDP ingest stream.
    uint32_t TotalBufferSize() const { return m_totalBufferSize; }

    /// @brief Set the total buffer size for each UDP ingest stream.
    /// @param totalBufferSize The total buffer size for each UDP ingest stream.
    void SetTotalBufferSize(uint32_t totalBufferSize) { m_totalBufferSize = totalBufferSize; }

    /// @brief Get the high-water mark for each UDP ingest stream where it writes to disk if it reaches this size.
    uint32_t HighWaterMark() const { return m_highWaterMark; }

    /// @brief Set the high-water mark for each UDP ingest stream where it writes to disk if it reaches this size.
    /// @param highWaterMark The high-water mark for each UDP ingest stream where it writes to disk if it reaches this size.
    void SetHighWaterMark(uint32_t highWaterMark) { m_highWaterMark = highWaterMark; }

  protected:
    /// @brief Default constructor
    PersistentState() : m_storingAtRestart(false), m_diskBlockSize(1024)
      , m_majorVersion(0), m_minorVersion(0), m_patchVersion(0)
      , m_totalBufferSize(512*1024), m_highWaterMark(512*1024 - 9000) {};

    /// File that we are associated with.
    std::string m_fileName;

    /// Version number of API stored in the files in this directory.
    uint16_t m_majorVersion, m_minorVersion, m_patchVersion;

    /// Are we storing to disk at restart?
    bool m_storingAtRestart;

    /// Number of bytes in a disk block.
    uint32_t m_diskBlockSize;

    /// Total buffer size for each UDP ingest stream
    uint32_t m_totalBufferSize;

    /// High-water mark for each UDP ingest stream where it writes to disk if it reaches this size.
    uint32_t m_highWaterMark;
  };
  PersistentState m_persistentState;

  //=============================================================================
  // Storage management.

  /// The root directory for storage.
  std::string m_storageRoot;

  /// The number of cameras on the server we last connected to.
  size_t m_numCameras;

  std::mutex m_storageMutex;
  std::vector< std::shared_ptr<SenderFile> > m_storageSenders;
  uint16_t m_writingToID;   ///< The ID of the session we are writing, 0 for none

  /// @brief Start storing to disk.
  /// @return Status indicating success or failure.
  Status StartStoring();

  /// @brief Stop storing to disk.
  /// @return Status indicating success or failure.
  Status StopStoring();

  //=============================================================================
  // Helper functions

  /// @brief Wait for a message of the specified type to arrive.
  /// @param type The type of message to wait for.
  /// @param seconds The number of seconds to wait for the message.
  /// @return The message that arrived, or nullptr if none arrived.
  std::shared_ptr<Message> WaitForMessageType(MessageID type, float seconds);

  //=============================================================================
  // Thread management.

  /// Set to true to stop all threads.
  std::atomic<bool> m_stop;

  /// @brief Information about a single server.
  struct ServerInfo {
    /// @brief Constructor
    ServerInfo(uint32_t SerialNumber, std::shared_ptr<Storage_Module_Server> Server) :
      m_serialNumber(SerialNumber), m_server(Server) { }

    uint32_t m_serialNumber;  ///< The serial number of the server.
    std::shared_ptr<Storage_Module_Server> m_server;  ///< The server.
  };

  /// One server per serial number found in the storage root directory.
  /// When we connect a client, we will also ensure that the server for that serial number is running.
  /// This is only adjusted in the constuctor before starting the client thread and then in the client thread,
  /// so we do not need to protect it with a mutex.
  std::vector< std::shared_ptr<ServerInfo> > m_servers;

  /// Server threads
  std::vector<std::thread> m_server_threads;

  /// @brief Body of a thread that handles a single server.
  /// @param server The server to handle.
  void ServerThread(std::shared_ptr<ServerInfo> server);

  /// Thread for the client to the Core Module.
  std::thread m_client_thread;

  /// @brief Body of the thread that handles the client connected to a Core Module.
  void ClientThread();

  /// Receivers for the UDP streams from each camera on the connected server.
  std::vector< std::shared_ptr<ReceiverUDP> > m_receivers;

  /// @brief Information about a single receiver.
  struct ReceiverInfo {
    /// @brief Constructor
    ReceiverInfo(std::shared_ptr<ReceiverUDP> Receiver, uint32_t ID) :
      m_receiver(Receiver), m_ID(ID) { }

    std::shared_ptr<ReceiverUDP> m_receiver;  ///< The receiver.
    uint32_t m_ID;  ///< The ID of the receiver.
  };

  std::vector<std::thread> m_receiver_threads;

  /// @brief Body of a thread that handles a single receiver.
  /// 
  /// This reads Messages from the receiver. If we are storing, it
  /// stores to the appropriate file. If we are streaming, it forwards
  /// the message to the appropriate m_server.
  /// @param receiver The receiver to handle.
  void StreamReceiverThread(std::shared_ptr<ReceiverInfo> receiver);

  /// The server associated with the current client.
  /// This is set in the client thread and not destroyed until all using threads are done and
  /// the server is mutexing the things that are needed to make calls thread safe, so we should not
  /// need to protect it with a mutex.
  std::shared_ptr<Storage_Module_Server> m_server;

  /// @brief Construct a new server thread based on the one we are connected to.
  /// 
  /// This not only makes a server object, but also starts a thread to run it.
  /// Before doing so, it creates a new directory under the storage root directory for the serial number
  /// and initializes the information in that directory.
  /// @param [out] server The server that was constructed as part of this.
  /// @return Status indicating success or failure.
  Status ConstructNewServer(std::shared_ptr<Storage_Module_Server> &server);

  /// @brief Configure the client connection based on the response from the Core Module.
  /// 
  /// This will request streaming of all optional streams that are supported by the server.
  /// It will also open a UDP receiver for each camera, at full size and rate.
  /// @param response The response from the Core Module.
  /// @return Status indicating success or failure.
  Status ConfigureClientConnection(const MessageState &response);

  /// Used to determine the port to listen on so that each is different.  Decrement to get the next port.
  std::atomic<uint16_t> m_nextPort;

  friend class Storage_Module_Server;
};

} // namespace asdp
