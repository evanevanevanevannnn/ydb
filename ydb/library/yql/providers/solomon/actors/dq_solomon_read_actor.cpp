#include "dq_solomon_read_actor.h"
#include "dq_solomon_actors_util.h"

#include <library/cpp/json/json_reader.h>
#include <library/cpp/protobuf/util/pb_io.h>
#include <library/cpp/retry/retry.h>

#include <util/string/join.h>
#include <ydb/library/yql/dq/actors/common/retry_queue.h>
#include <ydb/library/yql/providers/solomon/actors/dq_solomon_metrics_queue.h>
#include <ydb/library/yql/providers/solomon/common/constants.h>
#include <ydb/library/yql/providers/solomon/events/events.h>
#include <ydb/library/yql/providers/solomon/scheme/yql_solomon_scheme.h>
#include <ydb/library/yql/providers/solomon/solomon_accessor/client/solomon_accessor_client.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>
#include <ydb/library/yql/dq/actors/protos/dq_events.pb.h>
#include <ydb/library/yql/dq/actors/compute/dq_checkpoints_states.h>

#include <yql/essentials/minikql/comp_nodes/mkql_saveload.h>
#include <yql/essentials/minikql/mkql_alloc.h>
#include <yql/essentials/minikql/mkql_program_builder.h>
#include <yql/essentials/minikql/mkql_string_util.h>

#include <yql/essentials/public/issue/yql_issue_message.h>
#include <yql/essentials/public/udf/udf_data_type.h>

#include <ydb/library/yql/utils/actor_log/log.h>
#include <ydb/library/yql/utils/actors/http_sender_actor.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/url_builder.h>
#include <yql/essentials/utils/yql_panic.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/http/http_proxy.h>


#include <util/generic/algorithm.h>
#include <util/generic/hash.h>
#include <util/generic/size_literals.h>
#include <util/system/compiler.h>

#include <map>
#include <set>
#include <unordered_map>
#include <vector>

#define SOURCE_LOG_T(s) \
    LOG_TRACE_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SOURCE_LOG_D(s) \
    LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SOURCE_LOG_I(s) \
    LOG_INFO_S(*NActors::TlsActivationContext,  NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SOURCE_LOG_W(s) \
    LOG_WARN_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SOURCE_LOG_N(s) \
    LOG_NOTICE_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SOURCE_LOG_E(s) \
    LOG_ERROR_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SOURCE_LOG_C(s) \
    LOG_CRIT_S(*NActors::TlsActivationContext,  NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SOURCE_LOG(prio, s) \
    LOG_LOG_S(*NActors::TlsActivationContext, prio, NKikimrServices::KQP_COMPUTE, LogPrefix << s)

namespace NYql::NDq {

using namespace NActors;
using namespace NLog;
using namespace NKikimr::NMiniKQL;

namespace {

class TDqSolomonReadActor : public NActors::TActorBootstrapped<TDqSolomonReadActor>, public IDqComputeActorAsyncInput {
private:
    class TSizedSelectors {
    public:
        TSizedSelectors() {}
        TSizedSelectors(NSo::TSelectors&& selectors)
            : Selectors(std::move(selectors)) {
            for (const auto& [key, selector] : Selectors) {
                TotalSize += key.size() + selector.Op.size() + selector.Value.size();
            }
        }

        operator NSo::TSelectors() const {
            return Selectors;
        }

    public:
        NSo::TSelectors Selectors;
        ui64 TotalSize = 0;
    };

    class TSizedTimeseries {
    public:
        TSizedTimeseries(NSo::TTimeseries&& timeseries)
            : Timeseries(std::move(timeseries)) {
            for (const auto& [key, selector] : Timeseries.Metric.Selectors) {
                TotalSize += key.size() + selector.Op.size() + selector.Value.size();
            }
            TotalSize += Timeseries.Metric.Type.size();
            TotalSize += Timeseries.Timestamps.size() * sizeof(int64_t);
            TotalSize += Timeseries.Values.size() * sizeof(double);
        }

    public:
        NSo::TTimeseries Timeseries;
        ui64 TotalSize = 0;
    };

public:
    static constexpr char ActorName[] = "DQ_SOLOMON_READ_ACTOR";

    TDqSolomonReadActor(
        ui64 inputIndex,
        TCollectStatsLevel statsLevel,
        const TTxId& txId,
        const NActors::TActorId& computeActorId,
        const THolderFactory& holderFactory,
        NKikimr::NMiniKQL::TProgramBuilder& programBuilder,
        TDqSolomonReadParams&& readParams,
        ui64 metricsQueueConsumersCountDelta,
        NActors::TActorId metricsQueueActor,
        IMemoryQuotaManager::TPtr memoryQuotaManager,
        const ::NMonitoring::TDynamicCounterPtr& counters,
        std::shared_ptr<NYdb::ICredentialsProvider> credentialsProvider,
        const NSo::TSolomonReadActorConfig& cfg
        )
        : InputIndex(inputIndex)
        , TxId(txId)
        , ComputeActorId(computeActorId)
        , HolderFactory(holderFactory)
        , ProgramBuilder(programBuilder)
        , LogPrefix(TStringBuilder() << "TxId: " << TxId << ", TDqSolomonReadActor: ")
        , ReadParams(std::move(readParams))
        , ComputeActorBatchSize(cfg.ComputeActorBatchSize)
        , MetricsQueueConsumersCountDelta(metricsQueueConsumersCountDelta)
        , MaxApiInflight(cfg.MaxApiInflight)
        , MaxDataInflightBytes(cfg.MaxDataInflightMb * 1_MB)
        , MaxMetadataInflightBytes(cfg.MaxMetadataInflightMb * 1_MB)
        , MaxPointsPerOneRequest(cfg.MaxPointsPerOneRequest)
        , MaxSelectorsPerBatch(cfg.MaxSelectorsPerBatch)
        , TruePointsFindRangeSec(cfg.TruePointsFindRangeSec)
        , DownsamplingEnabled(!ReadParams.Source.GetDownsampling().GetDisabled())
        , MetricsQueueActor(metricsQueueActor)
        , MemoryQuotaManager(memoryQuotaManager)
        , CredentialsProvider(credentialsProvider)
        , SolomonClient(NSo::ISolomonAccessorClient::Make(ReadParams.Source, CredentialsProvider, cfg))
    {
        assert(MaxPointsPerOneRequest != 0);
        Y_UNUSED(counters);
        SOURCE_LOG_D("Init");
        IngressStats.Level = statsLevel;

        RetryPolicy = IRetryPolicy<NSo::TGetDataResponse>::GetExponentialBackoffPolicy(
            [](const NSo::TGetDataResponse& response) {
                if (response.Status == NSo::EStatus::STATUS_RETRIABLE_ERROR) {
                    return ERetryErrorClass::ShortRetry;
                }
                return ERetryErrorClass::NoRetry;
            },
            cfg.RetryConfig.MinDelay,
            cfg.RetryConfig.MinLongRetryDelay,
            cfg.RetryConfig.MaxDelay,
            cfg.RetryConfig.MaxRetries,
            cfg.RetryConfig.MaxTime
        );

        UseMetricsQueue = ReadParams.Source.HasSelectors();

        if (!UseMetricsQueue) {
            // Limited mode: a single user-provided program over the full range.
            // It is not split into sub-ranges (we do not know the point count
            // for an arbitrary program). Issue exactly one data request.
            TDataRequest req;
            req.RequestId = ++NextRequestId;
            req.Range = {
                TInstant::Seconds(ReadParams.Source.GetFrom()),
                TInstant::Seconds(ReadParams.Source.GetTo()),
            };
            req.Program = ReadParams.Source.GetProgram();
            req.InflightBytes = 0;
            req.State = RetryPolicy->CreateRetryState();

            PendingDataRequests[req.RequestId] = std::move(req);

            ListedTimeRanges = 1;
        }

        FillSystemFields();
    }

    void FillSystemFields() {
        YQL_ENSURE(ReadParams.Source.GetLabelNameAliases().size() == ReadParams.Source.GetLabelNames().size());

        // AliasIndex
        for (int i = 0; i < ReadParams.Source.GetLabelNameAliases().size(); ++i) {
            AliasIndex[ReadParams.Source.GetLabelNameAliases()[i]] = ReadParams.Source.GetLabelNames()[i];
        }

        // Index
        std::vector<TString> names(ReadParams.Source.GetSystemColumns().begin(), ReadParams.Source.GetSystemColumns().end());
        names.insert(names.end(), ReadParams.Source.GetLabelNameAliases().begin(), ReadParams.Source.GetLabelNameAliases().end());
        std::sort(names.begin(), names.end());
        size_t index = 0;
        for (auto& n : names) {
            Index[n] = index++;
        }

        // SharedRanges
        const TInstant extendedFrom = TInstant::Seconds(ReadParams.Source.GetFrom()) - TDuration::Seconds(TruePointsFindRangeSec);
        const TInstant extendedTo = TInstant::Seconds(ReadParams.Source.GetTo()) + TDuration::Seconds(TruePointsFindRangeSec);
        const TInstant cutoff = TInstant::Now() - NSo::NConstants::DownsamplingCutoff;
        const TInstant historicalEnd = std::min(std::max(cutoff, extendedFrom), extendedTo);
        
        NonDownsampledRange = {historicalEnd, extendedTo};

        if (extendedFrom < historicalEnd) {
            auto ranges = SplitDownsampledRange({extendedFrom, historicalEnd}, NSo::NConstants::DefaultGridInterval.MilliSeconds());
            SharedRanges.insert(SharedRanges.end(), ranges.begin(), ranges.end());
        }

        if (historicalEnd < extendedTo && DownsamplingEnabled) {
            const ui64 userGridMs = ReadParams.Source.GetDownsampling().GetGridMs();
            auto ranges = SplitDownsampledRange({historicalEnd,extendedTo}, userGridMs);
            SharedRanges.insert(SharedRanges.end(), ranges.begin(), ranges.end());
        }


        // Precomputed types
        auto stringType = ProgramBuilder.NewDataType(NYql::NUdf::TDataType<char*>::Id);
        DictType = ProgramBuilder.NewDictType(stringType, stringType, false);

        // [DIAG] Log config and time-range split summary
        SOURCE_LOG_I("[DIAG] Config: downsamplingEnabled=" << DownsamplingEnabled
            << ", MaxApiInflight=" << MaxApiInflight
            << ", MaxSelectorsPerBatch=" << MaxSelectorsPerBatch
            << ", MaxPointsPerOneRequest=" << MaxPointsPerOneRequest
            << ", MaxDataInflightBytes=" << MaxDataInflightBytes
            << ", TruePointsFindRangeSec=" << TruePointsFindRangeSec);
        SOURCE_LOG_I("[DIAG] TimeRanges: SharedRanges=" << SharedRanges.size() << " sub-ranges"
            << ", NonDownsampledRange=[" << NonDownsampledRange.From << ".." << NonDownsampledRange.To << ")"
            << ", queryRange=[" << TInstant::Seconds(ReadParams.Source.GetFrom()) << ".." << TInstant::Seconds(ReadParams.Source.GetTo()) << ")"
            << ", extendedRange=[" << extendedFrom << ".." << extendedTo << ")");
        for (size_t i = 0; i < SharedRanges.size(); ++i) {
            SOURCE_LOG_I("[DIAG]   SharedRange[" << i << "]: ["
                << SharedRanges[i].first.From << ".." << SharedRanges[i].first.To << ")"
                << ", estimatedPoints=" << SharedRanges[i].second);
        }
    }

    void Bootstrap() {
        SOURCE_LOG_D("Bootstrap");
        
        if (!MemoryQuotaManager->AllocateQuota(MaxDataInflightBytes + MaxMetadataInflightBytes)) {
            TIssues issues;
            issues.AddIssue(TIssue{TStringBuilder() << "OutOfMemory - can't allocate " << MaxDataInflightBytes + MaxMetadataInflightBytes << "b read buffer"});
            Send(ComputeActorId, new TEvAsyncInputError(InputIndex, issues, NYql::NDqProto::StatusIds::BAD_REQUEST));
            return;
        }

        if (UseMetricsQueue) {
            Become(&TDqSolomonReadActor::LimitlessModeState);
            MetricsQueueEvents.Init(TxId, SelfId(), SelfId());
            MetricsQueueEvents.OnNewRecipientId(MetricsQueueActor);

            if (MetricsQueueConsumersCountDelta > 0) {
                MetricsQueueEvents.Send(new TEvSolomonProvider::TEvUpdateConsumersCount(MetricsQueueConsumersCountDelta));
            }

            RequestMetrics();
        } else {
            Become(&TDqSolomonReadActor::LimitedModeState);
            SendDataRequest(PendingDataRequests.begin()->first);
        }

        Bootstrapped = true;

        // [DIAG] Log bootstrap mode
        SOURCE_LOG_I("[DIAG] Bootstrap: mode=" << (UseMetricsQueue ? "limitless(selectors)" : "limited(program)"));
    }
    
    STRICT_STFUNC(LimitlessModeState,
        hFunc(TEvSolomonProvider::TEvMetricsBatch, HandleMetricsBatch);
        hFunc(TEvSolomonProvider::TEvMetricsReadError, HandleMetricsReadError);
        hFunc(TEvSolomonProvider::TEvPointsCountBatch, HandlePointsCountBatch);
        hFunc(TEvSolomonProvider::TEvNewDataBatch, HandleNewDataBatch);
        hFunc(TEvSolomonProvider::TEvRetryDataRequest, HandleRetryDataRequest);
        hFunc(TEvSolomonProvider::TEvAck, Handle);
        hFunc(NYql::NDq::TEvRetryQueuePrivate::TEvRetry, Handle);
        hFunc(NActors::TEvInterconnect::TEvNodeDisconnected, Handle);
        hFunc(NActors::TEvInterconnect::TEvNodeConnected, Handle);
        hFunc(NActors::TEvents::TEvUndelivered, Handle);
    )

    STRICT_STFUNC(LimitedModeState,
        hFunc(TEvSolomonProvider::TEvNewDataBatch, HandleNewDataBatchLimited);
        hFunc(TEvSolomonProvider::TEvRetryDataRequest, HandleRetryDataRequest);
    )

    void HandleMetricsBatch(TEvSolomonProvider::TEvMetricsBatch::TPtr& metricsBatch) {
        if (!MetricsQueueEvents.OnEventReceived(metricsBatch)) {
            return;
        }

        if (!IsWaitingMetricsQueueResponse) {
            SOURCE_LOG_W("HandleMetricsBatch: unexpected metrics batch received while not waiting, dropping");
            return;
        }
        IsWaitingMetricsQueueResponse = false;
        auto& batch = metricsBatch->Get()->Record;
        IsMetricsQueueEmpty = batch.GetNoMoreMetrics();
        if (IsMetricsQueueEmpty && !IsConfirmedMetricsQueueFinish) {
            SOURCE_LOG_D("HandleMetricsBatch MetricsQueue empty, sending finish confirmation");
            RequestMetrics();
            IsConfirmedMetricsQueueFinish = true;
        }

        IngressStats.Bytes += batch.GetDownloadedBytes();
        IngressStats.Chunks++;
        IngressStats.Resume();
        auto& listedMetrics = batch.GetMetrics();

        SOURCE_LOG_D("HandleMetricsBatch batch of size " << listedMetrics.size());
        ListedMetricsCount += listedMetrics.size();

        for (const auto& metric : listedMetrics) {
            TSizedSelectors selectors(NSo::ProtoToSelectors(metric.GetSelectors()));

            for (const auto& [range, pointsCount] : SharedRanges) {
                PendingByRange[range].push_back({selectors, pointsCount});
                CurrentMetadataBytes += selectors.TotalSize;
                ListedTimeRanges++;
            }

            if (DownsamplingEnabled || NonDownsampledRange.From == NonDownsampledRange.To) {
                CompletedMetricsCount++;
            } else {
                CurrentMetadataBytes += selectors.TotalSize;
                ListedMetrics.emplace_back(std::move(selectors));
            }
        }

        while (TryRequestData()) {}

        if (LastMetricProcessed()) {
            NotifyComputeActorWithData();
        }
    }

    void HandleMetricsReadError(TEvSolomonProvider::TEvMetricsReadError::TPtr& metricsReadError) {
        if (!MetricsQueueEvents.OnEventReceived(metricsReadError)) {
            return;
        }

        IsMetricsQueueEmpty = true;
        if (!IsConfirmedMetricsQueueFinish) {
            SOURCE_LOG_D("HandleMetricsReadError sending finish confirmation to MetricsQueue");
            RequestMetrics();
            IsConfirmedMetricsQueueFinish = true;
        }

        TIssues issues { TIssue(metricsReadError->Get()->Record.GetIssues()) };
        SOURCE_LOG_W("Got " << "error list metrics response[" << metricsReadError->Cookie << "] from solomon: " << issues.ToOneLineString());
        Send(ComputeActorId, new TEvAsyncInputError(InputIndex, issues, NYql::NDqProto::StatusIds::BAD_REQUEST));
        return;
    }

    void HandlePointsCountBatch(TEvSolomonProvider::TEvPointsCountBatch::TPtr& pointsCountBatch) {
        auto& batch = *pointsCountBatch->Get();

        auto pendingIt = PendingPointsCountRequests.find(batch.RequestId);
        YQL_ENSURE(pendingIt != PendingPointsCountRequests.end());

        const auto& request = pendingIt->second;

        if (batch.Response.Status != NSo::EStatus::STATUS_OK) {
            TIssues issues { TIssue(batch.Response.Error) };
            SOURCE_LOG_W("Got " << "error points count response[" << pointsCountBatch->Cookie << "] from solomon: " << issues.ToOneLineString());
            Send(ComputeActorId, new TEvAsyncInputError(InputIndex, issues, NYql::NDqProto::StatusIds::BAD_REQUEST));
            return;
        }

        IngressStats.Bytes += batch.Response.DownloadedBytes;
        IngressStats.Chunks++;
        IngressStats.Resume();

        auto& selectors = request.Selectors;
        auto& pointsCount = batch.Response.Result.PointsCount;
        
        auto ranges = NSo::SplitIntoRanges(NonDownsampledRange, pointsCount, MaxPointsPerOneRequest);
        for (const auto& [range, pointsCount] : ranges) {
            PendingByRange[range].push_back({selectors, pointsCount});
            CurrentMetadataBytes += selectors.TotalSize;
            ListedTimeRanges++;
        }

        CompletedMetricsCount++;
        PendingPointsCountRequests.erase(pendingIt);

        while (TryRequestData()) {}
    }

    void HandleNewDataBatch(TEvSolomonProvider::TEvNewDataBatch::TPtr& newDataBatch) {
        if (!SaveDataBatch(newDataBatch)) {
            return;
        }

        if (!PendingByRange.empty()) {
            while (TryRequestData()) {}
        }
        if (MetricsData.size() >= ComputeActorBatchSize
            || CurrentDataBytesStored > MaxDataInflightBytes
            || LastMetricProcessed())
        {
            NotifyComputeActorWithData();
        }
    }

    void HandleRetryDataRequest(TEvSolomonProvider::TEvRetryDataRequest::TPtr& retryDataRequest) {
        SendDataRequest(std::move(retryDataRequest->Get()->RequestId));
    }

    void Handle(TEvSolomonProvider::TEvAck::TPtr& ev) {
        MetricsQueueEvents.OnEventReceived(ev);
    }

    void Handle(const NYql::NDq::TEvRetryQueuePrivate::TEvRetry::TPtr&) {
        SOURCE_LOG_D("Handle MetricsQueue retry");
        MetricsQueueEvents.Retry();
    }

    void Handle(NActors::TEvInterconnect::TEvNodeDisconnected::TPtr& ev) {
        SOURCE_LOG_D("Handle MetricsQueue disconnected " << ev->Get()->NodeId);
        MetricsQueueEvents.HandleNodeDisconnected(ev->Get()->NodeId);
    }

    void Handle(NActors::TEvInterconnect::TEvNodeConnected::TPtr& ev) {
        SOURCE_LOG_D("Handle MetricsQueue connected " << ev->Get()->NodeId);
        MetricsQueueEvents.HandleNodeConnected(ev->Get()->NodeId);
    }

    void Handle(NActors::TEvents::TEvUndelivered::TPtr& ev) {
        SOURCE_LOG_D("Handle MetricsQueue undelivered");
        if (MetricsQueueEvents.HandleUndelivered(ev) != NYql::NDq::TRetryEventsQueue::ESessionState::WrongSession) {
            TIssues issues{TIssue{TStringBuilder() << "MetricsQueue was lost"}};
            Send(ComputeActorId, new TEvAsyncInputError(InputIndex, issues, NYql::NDqProto::StatusIds::UNAVAILABLE));
        }
    }

    void HandleNewDataBatchLimited(TEvSolomonProvider::TEvNewDataBatch::TPtr& newDataBatch) {
        if (!SaveDataBatch(newDataBatch)) {
            return;
        }

        if (MetricsData.size() >= ComputeActorBatchSize
            || CurrentDataBytesStored > MaxDataInflightBytes
            || LastMetricProcessed())
        {
            NotifyComputeActorWithData();
        }
    }

    i64 GetAsyncInputData(TUnboxedValueBatch& buffer, TMaybe<TInstant>&, bool& finished, i64) final {
        i64 total = 0;
        YQL_ENSURE(!buffer.IsWide(), "Wide stream is not supported");
        SOURCE_LOG_D("GetAsyncInputData sending " << MetricsData.size() << " metrics, finished = " << LastMetricProcessed());

        TInstant from = TInstant::Seconds(ReadParams.Source.GetFrom());
        TInstant to = TInstant::Seconds(ReadParams.Source.GetTo());

        for (const auto& data : MetricsData) {
            total += data.TotalSize;

            auto& labels = data.Timeseries.Metric.Selectors;

            auto dictValueBuilder = HolderFactory.NewDict(DictType, 0);
            for (auto& [key, value] : labels) {
                dictValueBuilder->Add(NKikimr::NMiniKQL::MakeString(key), NKikimr::NMiniKQL::MakeString(value.Value));
            }
            auto dictValue = dictValueBuilder->Build();

            auto& timestamps = data.Timeseries.Timestamps;
            auto& values = data.Timeseries.Values;
            auto& type = data.Timeseries.Metric.Type;

            for (size_t i = 0; i < timestamps.size(); ++i){
                TInstant timestamp = TInstant::MilliSeconds(timestamps[i]);
                if (timestamp < from || timestamp >= to) {
                    continue;
                }

                NUdf::TUnboxedValue* items = nullptr;
                auto value = HolderFactory.CreateDirectArrayHolder(ReadParams.Source.GetSystemColumns().size() + ReadParams.Source.GetLabelNames().size(), items);

                if (auto it = Index.find(SOLOMON_SCHEME_VALUE); it != Index.end()) {
                    items[it->second] = isnan(values[i]) ? NUdf::TUnboxedValuePod() : NUdf::TUnboxedValuePod(values[i]).MakeOptional();
                }

                if (auto it = Index.find(SOLOMON_SCHEME_TYPE); it != Index.end()) {
                    items[it->second] = NKikimr::NMiniKQL::MakeString(type);
                }

                if (auto it = Index.find(SOLOMON_SCHEME_TS); it != Index.end()) {
                    // convert ms to sec
                    items[it->second] = NUdf::TUnboxedValuePod((ui64)timestamps[i] / 1000);
                }

                if (auto it = Index.find(SOLOMON_SCHEME_LABELS); it != Index.end()) {
                    items[it->second] = dictValue;
                }

                for (const auto& c : ReadParams.Source.GetLabelNameAliases()) {
                    auto& v = items[Index[c]];
                    auto it = labels.find(AliasIndex[c]);
                    if (it != labels.end()) {
                        v = NKikimr::NMiniKQL::MakeString(it->second.Value);
                    } else {
                        // empty string
                        v = NKikimr::NMiniKQL::MakeString("");
                    }
                }

                buffer.push_back(value);
            }

            CurrentDataBytesStored -= data.TotalSize;
            TryRequestData();
        }

        finished = LastMetricProcessed();
        if (MetricsData.empty()) {
            IngressStats.TryPause();
        }

        MetricsData.clear();
        return total;
    }

    // Checkpointing is not supported for the Solomon source: it performs a
    // bounded, non-restartable scan of a fixed time range.
    void SaveState(const NDqProto::TCheckpoint&, TSourceState&) final {}
    void LoadState(const TSourceState&) override {}
    void CommitState(const NDqProto::TCheckpoint&) override {}

    ui64 GetInputIndex() const override {
        return InputIndex;
    }

    const TDqAsyncStats& GetIngressStats() const override {
        return IngressStats;
    }

private:
    // IActor & IDqComputeActorAsyncInput
    void PassAway() override { // Is called from Compute Actor
        SOURCE_LOG_I("PassAway, processed " << CompletedMetricsCount << " metrics, " << CompletedTimeRanges << " time ranges.");
        // [DIAG] Final batching efficiency summary
        {
            const double avgPoints = DiagTotalDataRequests > 0
                ? static_cast<double>(DiagTotalPointsReturned) / DiagTotalDataRequests : 0;
            const double avgSelectors = DiagTotalDataRequests > 0
                ? static_cast<double>(DiagTotalSelectorsInRequests) / DiagTotalDataRequests : 0;
            SOURCE_LOG_N("[DIAG] SUMMARY: totalDataRequests=" << DiagTotalDataRequests
                << ", totalPointsReturned=" << DiagTotalPointsReturned
                << ", avgPointsPerRequest=" << avgPoints
                << ", minPointsPerRequest=" << (DiagMinPointsPerRequest == Max<ui64>() ? 0 : DiagMinPointsPerRequest)
                << ", maxPointsPerRequest=" << DiagMaxPointsPerRequest
                << ", avgSelectorsPerRequest=" << avgSelectors
                << ", minSelectorsPerRequest=" << (DiagMinSelectorsPerRequest == Max<ui64>() ? 0 : DiagMinSelectorsPerRequest)
                << ", maxSelectorsPerRequest=" << DiagMaxSelectorsPerRequest
                << ", listedMetrics=" << ListedMetricsCount
                << ", completedTimeRanges=" << CompletedTimeRanges << "/" << ListedTimeRanges
                << ", sharedRangesCount=" << SharedRanges.size());
        }
        if (Bootstrapped) {
            if (UseMetricsQueue) {
                if (!IsConfirmedMetricsQueueFinish) {
                    SOURCE_LOG_D("PassAway sending consumer-finished notification to MetricsQueue");
                    MetricsQueueEvents.Send(new TEvSolomonProvider::TEvConsumerFinished());
                    IsConfirmedMetricsQueueFinish = true;
                }
                MetricsQueueEvents.Unsubscribe();
            }

            if (MemoryQuotaManager) {
                MemoryQuotaManager->FreeQuota(MaxDataInflightBytes + MaxMetadataInflightBytes);
            }
        }
        // Explicitly destroy the client before the actor dies.
        // This calls ~TSolomonAccessorClient() → GrpcClient->Stop(), which cancels
        // all in-flight gRPC contexts synchronously. Without this, the contexts would
        // only be cancelled when the shared_ptr ref-count drops to zero after actor
        // destruction, which may race with the actor system freeing actor memory.
        SolomonClient.reset();
        MemoryQuotaManager.reset();
        TActor<TDqSolomonReadActor>::PassAway();
    }

private:
    TSourceState BuildState() { return {}; }

    void NotifyComputeActorWithData() const {
        SOURCE_LOG_D("NotifyComputeActorWithData");
        Send(ComputeActorId, new TEvNewAsyncInputDataArrived(InputIndex));
    }

    bool LastMetricProcessed() const {
        if (UseMetricsQueue) {
            return IsMetricsQueueEmpty && CompletedMetricsCount == ListedMetricsCount && CompletedTimeRanges == ListedTimeRanges;
        }
        return CompletedTimeRanges == ListedTimeRanges;
    }

    void TryRequestMetrics() {
        if (IsMetricsQueueEmpty || IsWaitingMetricsQueueResponse) {
            return;
        }
        if (CurrentMetadataBytes >= MaxMetadataInflightBytes) {
            return;
        }
        RequestMetrics();
    }

    void RequestMetrics() {
        MetricsQueueEvents.Send(new TEvSolomonProvider::TEvGetNextBatch());
        IsWaitingMetricsQueueResponse = true;
    }

    bool TryRequestPointsCount() {
        TryRequestMetrics();

        if (ListedMetrics.empty()) {
            return false;
        }

        if (CurrentMetadataBytes >= MaxMetadataInflightBytes) {
            return false;
        }

        RequestPointsCount();
        return true;
    }

    void RequestPointsCount() {
        TPointsCountRequest req;
        req.RequestId = ++NextRequestId;
        req.Selectors = std::move(ListedMetrics.back());
        ListedMetrics.pop_back();

        CurrentMetadataBytes -= std::min(CurrentMetadataBytes, req.Selectors.TotalSize);

        auto getPointsCountFuture = SolomonClient->GetPointsCount(req.Selectors, NonDownsampledRange);

        NActors::TActorSystem* actorSystem = NActors::TActivationContext::ActorSystem();
        getPointsCountFuture.Subscribe([actorSystem, requestId = req.RequestId, selfId = SelfId()](
        const NThreading::TFuture<NSo::TGetPointsCountResponse>& response) mutable -> void
        {
            actorSystem->Send(selfId, new TEvSolomonProvider::TEvPointsCountBatch(
                response.GetValue(),
                requestId
            ));
        });

        PendingPointsCountRequests[req.RequestId] = std::move(req);
    }

    // Returns the split for one grid-aligned half of the read window.
    std::vector<std::pair<NSo::TTimeRange, ui64>> SplitDownsampledRange(NSo::TTimeRange range, ui64 gridMs) const {
        ui64 totalPoints = 0;
        if (gridMs > 0) {
            totalPoints = static_cast<ui64>(
                ceil((range.To - range.From).Seconds() * 1000.0 / gridMs)) + 1;
        } else {
            totalPoints = MaxPointsPerOneRequest;
        }
        return NSo::SplitIntoRanges(range, totalPoints, MaxPointsPerOneRequest);
    }

    bool TryRequestData() {
        if (!DownsamplingEnabled) {
            TryRequestPointsCount();
        } else {
            TryRequestMetrics();
        }

        if (PendingByRange.empty()) {
            return false;
        }

        if (CurrentDataInflight >= MaxApiInflight) {
            return false;
        }

        if (CurrentDataBytesStored + CurrentDataBytesInflight >= MaxDataInflightBytes) {
            return false;
        }

        RequestData();
        return true;
    }

    void RequestData() {
        YQL_ENSURE(!PendingByRange.empty());
        YQL_ENSURE(RetryPolicy);

        auto bucketIt = PendingByRange.begin();
        for (auto it = std::next(PendingByRange.begin()); it != PendingByRange.end(); ++it) {
            if (it->second.size() > bucketIt->second.size()) {
                bucketIt = it;
            }
        }
        const NSo::TTimeRange range = bucketIt->first;
        auto& bucket = bucketIt->second;

        TDataRequest req;
        req.RequestId = ++NextRequestId;
        req.Range = range;
        req.SelectorsBatch.reserve(std::min<ui64>(bucket.size(), MaxSelectorsPerBatch));

        // Take lines until any of the limits would be exceeded; always take at
        // least one (Split guarantees per-line points <= MaxPointsPerOneRequest).
        // Refund the metadata budget per entry as we move it out of the bucket.
        ui64 takenPoints = 0;
        while (!bucket.empty() && req.SelectorsBatch.size() < MaxSelectorsPerBatch) {
            auto& [selectors, pointsCount] = bucket.back();
            if (takenPoints + pointsCount > MaxPointsPerOneRequest)
            {
                break;
            }
            takenPoints += pointsCount;
            CurrentMetadataBytes -= std::min(CurrentMetadataBytes, selectors.TotalSize);
            req.SelectorsBatch.push_back(std::move(selectors));
            bucket.pop_back();
        }

        if (bucket.empty()) {
            PendingByRange.erase(bucketIt);
        }

        req.InflightBytes = takenPoints * (sizeof(int64_t) + sizeof(double));
        req.State = RetryPolicy->CreateRetryState();

        // [DIAG] Log per-request batching details
        DiagTotalDataRequests++;
        DiagTotalSelectorsInRequests += req.SelectorsBatch.size();
        DiagMinSelectorsPerRequest = std::min(DiagMinSelectorsPerRequest, static_cast<ui64>(req.SelectorsBatch.size()));
        DiagMaxSelectorsPerRequest = std::max(DiagMaxSelectorsPerRequest, static_cast<ui64>(req.SelectorsBatch.size()));
        SOURCE_LOG_I("[DIAG] RequestData: reqId=" << req.RequestId
            << ", range=[" << range.From << ".." << range.To << ")"
            << ", durationSec=" << (range.To - range.From).Seconds()
            << ", selectorsInBatch=" << req.SelectorsBatch.size()
            << ", expectedPoints=" << takenPoints
            << ", pendingBuckets=" << PendingByRange.size()
            << ", currentInflight=" << CurrentDataInflight << "/" << MaxApiInflight);

        PendingDataRequests[req.RequestId] = std::move(req);

        SendDataRequest(req.RequestId);
    }

    void SendDataRequest(ui64 requestId) {
        // Register a retry state by RequestId on the first attempt only.
        auto pendingIt = PendingDataRequests.find(requestId);
        YQL_ENSURE(pendingIt != PendingDataRequests.end());

        const auto& request = pendingIt->second;

        CurrentDataInflight++;
        CurrentDataBytesInflight += request.InflightBytes;

        NThreading::TFuture<NSo::TGetDataResponse> dataRequestFuture;
        try {
            if (UseMetricsQueue) {
                dataRequestFuture = SolomonClient->GetData(request.SelectorsBatch, request.Range);
            } else {
                dataRequestFuture = SolomonClient->GetData(request.Program, request.Range);
            }
        } catch (const std::exception& ex) {
            dataRequestFuture = NThreading::MakeFuture(NSo::TGetDataResponse(TString(ex.what())));
        }

        dataRequestFuture.Subscribe([requestId,
                                     actorSystem = TActivationContext::ActorSystem(),
                                     selfId = SelfId()](
            NThreading::TFuture<NSo::TGetDataResponse> response) mutable -> void
        {
            actorSystem->Send(selfId, new TEvSolomonProvider::TEvNewDataBatch(
                response.ExtractValue(),
                requestId
            ));
        });
    }

    bool SaveDataBatch(TEvSolomonProvider::TEvNewDataBatch::TPtr& newDataBatch) {
        auto& batch = *newDataBatch->Get();
        
        auto pendingIt = PendingDataRequests.find(batch.RequestId);
        YQL_ENSURE(pendingIt != PendingDataRequests.end());

        auto& request = pendingIt->second;

        CurrentDataBytesInflight -= std::min(CurrentDataBytesInflight, request.InflightBytes);
        CurrentDataInflight--;

        if (batch.Response.Status == NSo::EStatus::STATUS_RETRIABLE_ERROR) {
            if (auto delay = request.State->GetNextRetryDelay(batch.Response)) {
                SOURCE_LOG_D("HandleNewDataBatch: retrying data request, delay: " << delay->MilliSeconds());
                // Refund inflight accounting; the retried request will re-add it.
                Schedule(*delay, new TEvSolomonProvider::TEvRetryDataRequest(request.RequestId));
                return false;
            }
        }
        if (batch.Response.Status != NSo::EStatus::STATUS_OK) {
            TIssues issues { TIssue(batch.Response.Error) };
            SOURCE_LOG_W("Got " << "error data response[" << newDataBatch->Cookie << "] from solomon: " << issues.ToOneLineString());
            Send(ComputeActorId, new TEvAsyncInputError(InputIndex, issues, NYql::NDqProto::StatusIds::BAD_REQUEST));
            return false;
        }

        IngressStats.Bytes += batch.Response.DownloadedBytes;
        IngressStats.Rows += batch.Response.Result.Timeseries.size();
        IngressStats.Chunks++;
        IngressStats.Resume();

        SOURCE_LOG_D(TStringBuilder() << "HandleNewDataBatch: got " << batch.Response.Result.Timeseries.size() << " metrics");

        for (auto& metric : batch.Response.Result.Timeseries) {
            MetricsData.emplace_back(std::move(metric));
            CurrentDataBytesStored += MetricsData.back().TotalSize;
        }

        // [DIAG] Count points in this response and update min/max stats
        {
            ui64 diagTimeseriesCount = batch.Response.Result.Timeseries.size();
            ui64 diagPointsInResponse = 0;
            // Timeseries were moved out, but we can count from the last N MetricsData entries
            auto it = MetricsData.end();
            for (ui64 i = 0; i < diagTimeseriesCount && it != MetricsData.begin(); ++i) {
                --it;
                diagPointsInResponse += it->Timeseries.Timestamps.size();
            }
            DiagTotalPointsReturned += diagPointsInResponse;
            DiagMinPointsPerRequest = std::min(DiagMinPointsPerRequest, diagPointsInResponse);
            DiagMaxPointsPerRequest = std::max(DiagMaxPointsPerRequest, diagPointsInResponse);
            SOURCE_LOG_I("[DIAG] DataBatchReceived: reqId=" << batch.RequestId
                << ", timeseriesCount=" << diagTimeseriesCount
                << ", pointsInResponse=" << diagPointsInResponse
                << ", completedRanges=" << (CompletedTimeRanges + std::max<ui64>(1, request.SelectorsBatch.size())) << "/" << ListedTimeRanges);
        }

        CompletedTimeRanges += std::max<ui64>(1, request.SelectorsBatch.size());
        PendingDataRequests.erase(pendingIt);

        return true;
    }

private:
    const ui64 InputIndex;
    TDqAsyncStats IngressStats;
    const TTxId TxId;
    const NActors::TActorId ComputeActorId;
    const THolderFactory& HolderFactory;
    NKikimr::NMiniKQL::TProgramBuilder& ProgramBuilder;
    const TString LogPrefix;
    const TDqSolomonReadParams ReadParams;
    const ui64 ComputeActorBatchSize;
    const ui64 MetricsQueueConsumersCountDelta;
    const ui64 MaxApiInflight;
    const ui64 MaxDataInflightBytes;
    const ui64 MaxMetadataInflightBytes;
    const ui64 MaxPointsPerOneRequest;
    const ui64 MaxSelectorsPerBatch;
    const ui64 TruePointsFindRangeSec;
    const bool DownsamplingEnabled;
    NSo::TTimeRange NonDownsampledRange;
    IRetryPolicy<NSo::TGetDataResponse>::TPtr RetryPolicy;

    bool Bootstrapped = false;
    bool UseMetricsQueue;
    TRetryEventsQueue MetricsQueueEvents;
    NActors::TActorId MetricsQueueActor;
    IMemoryQuotaManager::TPtr MemoryQuotaManager;
    bool IsWaitingMetricsQueueResponse = false;
    bool IsMetricsQueueEmpty = false;
    bool IsConfirmedMetricsQueueFinish = false;

    ui64 NextRequestId = 0;

    struct TPointsCountRequest {
        ui64 RequestId = 0;

        TSizedSelectors Selectors;
    };
    std::unordered_map<ui64, TPointsCountRequest> PendingPointsCountRequests;

    // Per in-flight request retry state + the inflight bytes booked on the
    // data-bytes budget (recorded so we can refund the exact amount on retry
    // / completion).
    struct TDataRequest {
        ui64 RequestId = 0;
        ui64 InflightBytes = 0;

        NSo::TTimeRange Range;
        std::vector<NSo::TSelectors> SelectorsBatch;
        TString Program; // non-empty only in "limited" (single-program) mode

        IRetryPolicy<NSo::TGetDataResponse>::IRetryState::TPtr State;
    };
    std::unordered_map<ui64, TDataRequest> PendingDataRequests;

    // Buffer of metrics awaiting GetPointsCount; only used when downsampling
    // is disabled. In downsampling mode metrics flow directly from
    // HandleMetricsBatch into PendingByRange, skipping this deque.
    std::deque<TSizedSelectors> ListedMetrics;
    // Pending lines grouped by (From, To). Each bucket entry is a (selectors,
    // per-line points-count) pair. The interval with the largest bucket is
    // chosen in RequestData() via a linear scan.
    std::map<NSo::TTimeRange, std::vector<std::pair<TSizedSelectors, ui64>>> PendingByRange;
    // Pre-computed sub-ranges shared across all metrics in the downsampling-
    // enabled mode (each metric has the same expected point count).
    std::vector<std::pair<NSo::TTimeRange, ui64>> SharedRanges;

    std::deque<TSizedTimeseries> MetricsData;
    ui64 ListedMetricsCount = 0;
    ui64 CompletedMetricsCount = 0;
    ui64 ListedTimeRanges = 0;
    ui64 CompletedTimeRanges = 0;
    // Decoded response payload waiting to be delivered to the compute actor
    // (bytes of timestamp+value pairs accumulated in MetricsData).
    ui64 CurrentDataBytesStored = 0;
    ui64 CurrentDataInflight = 0;
    // Sum of expected response sizes for outstanding GetData requests
    // (charged against MaxDataInflightBytes).
    ui64 CurrentDataBytesInflight = 0;
    // Selector bytes currently owned by ListedMetrics + PendingByRange.
    // The two containers share the MaxMetadataInflightBytes quota; nothing
    // else (response payload, request metadata, retry state) is accounted
    // here.
    ui64 CurrentMetadataBytes = 0;

    // [DIAG] Temporary diagnostic counters (remove with all [DIAG] code)
    ui64 DiagTotalDataRequests = 0;
    ui64 DiagTotalPointsReturned = 0;
    ui64 DiagMinPointsPerRequest = Max<ui64>();
    ui64 DiagMaxPointsPerRequest = 0;
    ui64 DiagTotalSelectorsInRequests = 0;
    ui64 DiagMinSelectorsPerRequest = Max<ui64>();
    ui64 DiagMaxSelectorsPerRequest = 0;

    TString SourceId;
    std::shared_ptr<NYdb::ICredentialsProvider> CredentialsProvider;
    NSo::ISolomonAccessorClient::TPtr SolomonClient;
    TType* DictType = nullptr;
    THashMap<TString, size_t> Index;
    THashMap<TString, TString> AliasIndex;
};


} // namespace

std::pair<NYql::NDq::IDqComputeActorAsyncInput*, NActors::IActor*> CreateDqSolomonReadActor(
    NYql::NSo::NProto::TDqSolomonSource&& source,
    ui64 inputIndex,
    ui64 metricsQueueConsumersCountDelta,
    TCollectStatsLevel statsLevel,
    const TTxId& txId,
    const NActors::TActorId& computeActorId,
    const NKikimr::NMiniKQL::THolderFactory& holderFactory,
    NKikimr::NMiniKQL::TProgramBuilder& programBuilder,
    const THashMap<TString, TString>& secureParams,
    IMemoryQuotaManager::TPtr memoryQuotaManager,
    const ::NMonitoring::TDynamicCounterPtr& counters,
    ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
    const NSo::TSolomonReadActorConfig& cfg)
{
    const TString& tokenName = source.GetToken().GetName();
    const TString token = secureParams.Value(tokenName, TString());

    TDqSolomonReadParams params {
        .Source = std::move(source),
    };

    auto& settings = params.Source.settings();

    NActors::TActorId metricsQueueActor;
    if (auto it = settings.find("metricsQueueActor"); it != settings.end()) {
        NActorsProto::TActorId protoId;
        TMemoryInput inputStream(it->second);
        ParseFromTextFormat(inputStream, protoId);
        metricsQueueActor = ActorIdFromProto(protoId);
    }

    auto credentialsProviderFactory = CreateCredentialsProviderFactoryForStructuredToken(credentialsFactory, token);
    auto credentialsProvider = credentialsProviderFactory->CreateProvider();

    TDqSolomonReadActor* actor = new TDqSolomonReadActor(
        inputIndex,
        statsLevel,
        txId,
        computeActorId,
        holderFactory,
        programBuilder,
        std::move(params),
        metricsQueueConsumersCountDelta,
        metricsQueueActor,
        memoryQuotaManager,
        counters,
        credentialsProvider,
        cfg);
    return {actor, actor};
}

void RegisterDQSolomonReadActorFactory(TDqAsyncIoFactory& factory, ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory) {
    factory.RegisterSource<NSo::NProto::TDqSolomonSource>("SolomonSource",
        [credentialsFactory](
            NYql::NSo::NProto::TDqSolomonSource&& settings,
            IDqAsyncIoFactory::TSourceArguments&& args)
        {
            auto counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();

            ui64 metricsQueueConsumersCountDelta = 0;
            if (args.ReadRanges.size() > 1) {
                metricsQueueConsumersCountDelta = args.ReadRanges.size() - 1;
            }

            const NSo::TSolomonReadActorConfig cfg = NSo::ParseSolomonReadActorConfig(settings.settings());

            return CreateDqSolomonReadActor(
                std::move(settings),
                args.InputIndex,
                metricsQueueConsumersCountDelta,
                args.StatsLevel,
                args.TxId,
                args.ComputeActorId,
                args.HolderFactory,
                args.ProgramBuilder,
                args.SecureParams,
                args.MemoryQuotaManager,
                counters,
                credentialsFactory,
                cfg);
        });
}

}
