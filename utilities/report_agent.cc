//
// Created by jinghuan on 5/24/21.
//

#include "rocksdb/utilities/report_agent.h"

namespace rocksdb {
ReporterAgent::~ReporterAgent() {
  {
    std::unique_lock<std::mutex> lk(mutex_);
    stop_ = true;
    stop_cv_.notify_all();
  }
  reporting_thread_.join();
}

Status ReporterAgent::ReportLine(int secs_elapsed,
                                 int total_ops_done_snapshot) {
  std::string report = std::to_string(secs_elapsed) + "," +
                       std::to_string(total_ops_done_snapshot - last_report_);
  auto s = report_file_->Append(report);
  return s;
}


Status SILK_pause_compaction(DBImpl* running_db_, bool* stopped) {
  Status s = running_db_->PauseBackgroundWork();
  if (!s.ok()) {
    printf("PauseBackgroundWork failed");
    exit(-1);
  }
  *stopped = true;
  return s;
}

Status SILK_resume_compaction(DBImpl* running_db_, bool* stopped) {
  Status s = running_db_->ContinueBackgroundWork();
  if (!s.ok()) {
    printf("ContinueBackgroundWork failed");
    exit(-1);
  }
  *stopped = false;
  return s;
}

Status ReporterAgentWithSILK::ReportLine(int secs_elapsed,
                                         int total_ops_done_snapshot) {
  // copy from SILK https://github.com/theoanab/SILK-USENIXATC2019
  // //check the current bandwidth for user operations
  long cur_throughput = (total_ops_done_snapshot - last_report_);
  long cur_bandwidth_user_ops_MBPS =
      cur_throughput * FLAGS_value_size / 1000000;

  // SILK TESTING the Pause compaction work functionality
  if (!pausedcompaction &&
      cur_bandwidth_user_ops_MBPS > FLAGS_SILK_bandwidth_limitation * 0.75) {
    // SILK Consider this a load peak
    //    running_db_->PauseCompactionWork();
    //    pausedcompaction = true;
    // pausedcompaction = true;
    std::thread t(SILK_pause_compaction, running_db_, &pausedcompaction);
    t.detach();
    printf("->>>>??? Pausing compaction work from SILK\n");

  } else if (pausedcompaction && cur_bandwidth_user_ops_MBPS <=
                                     FLAGS_SILK_bandwidth_limitation * 0.75) {
    std::thread t(SILK_resume_compaction, running_db_, &pausedcompaction);
    t.detach();
    printf("->>>>??? Resuming compaction work from SILK\n");
  }

  long cur_bandiwdth_compaction_MBPS =
      FLAGS_SILK_bandwidth_limitation -
      cur_bandwidth_user_ops_MBPS;  // measured 200MB/s SSD bandwidth on XEON.
  if (cur_bandiwdth_compaction_MBPS < 10) {
    cur_bandiwdth_compaction_MBPS = 10;
  }
  if (abs(prev_bandwidth_compaction_MBPS - cur_bandiwdth_compaction_MBPS) >=
      10) {
    auto opt = running_db_->GetOptions();
    opt.rate_limiter->SetBytesPerSecond(cur_bandiwdth_compaction_MBPS *
                                              1000 * 1000);
    printf(
        "Adjust-compaction-rate; current-client-bandwidth: %d ops/s; "
        "Bandwidth-taken: %d MB/s; Left-for-compaction: %d\n",
        cur_throughput, cur_bandwidth_user_ops_MBPS,
        cur_bandiwdth_compaction_MBPS);
    prev_bandwidth_compaction_MBPS = cur_bandiwdth_compaction_MBPS;
  }
  // Adjust the tuner from SILK before reporting
  std::string report = std::to_string(secs_elapsed) + "," +
                       std::to_string(total_ops_done_snapshot - last_report_) +
                       "," + std::to_string(cur_bandwidth_user_ops_MBPS) + "," +
                       std::to_string(cur_bandiwdth_compaction_MBPS);
  auto s = report_file_->Append(report);
  return s;
}

ReporterAgentWithSILK::ReporterAgentWithSILK(DBImpl* running_db, Env* env,
                                             const std::string& fname,
                                             uint64_t report_interval_secs,
                                             int32_t value_size,
                                             int32_t bandwidth_limitation)
    : ReporterAgent(env, fname, report_interval_secs) {
  std::cout << "SILK enabled, Disk bandwidth has been set to: "
            << bandwidth_limitation << std::endl;
  running_db_ = running_db;
  this->FLAGS_value_size = value_size;
  this->FLAGS_SILK_bandwidth_limitation = bandwidth_limitation;
}

};  // namespace ROCKSDB_NAMESPACE
