#pragma once

#include <ydb/core/base/events.h>
#include <ydb/library/yql/providers/solomon/common/util.h>
#include <ydb/library/yql/providers/solomon/proto/metrics_queue.pb.h>
#include <ydb/library/yql/providers/solomon/solomon_accessor/client/solomon_accessor_client.h>

#include <library/cpp/retry/retry_policy.h>

namespace NYql::NDq {

struct TEvSolomonProvider {

    enum EEv : ui32 {
        EvBegin = EventSpaceBegin(NKikimr::TKikimrEvents::ES_SOLOMON_PROVIDER),

        // lister events
        EvUpdateConsumersCount = EvBegin,
        EvAck,
        EvGetNextBatch,
        EvMetricsBatch,
        EvMetricsReadError,
        EvConsumerFinished,

        // read actor events
        EvPointsCountBatch,
        EvNewDataBatch,
        EvRetryDataRequest,

        EvEnd
    };
    static_assert(EvEnd < EventSpaceEnd(NKikimr::TKikimrEvents::ES_SOLOMON_PROVIDER), "expect EvEnd < EventSpaceEnd(NKikimr::TKikimrEvents::ES_SOLOMON_PROVIDER)");

    struct TEvUpdateConsumersCount :
        public NActors::TEventPB<TEvUpdateConsumersCount, NSo::MetricQueue::TEvUpdateConsumersCount, EvUpdateConsumersCount> {
        
        explicit TEvUpdateConsumersCount(ui64 consumersCountDelta = 0) {
            Record.SetConsumersCountDelta(consumersCountDelta);
        }
    };

    struct TEvAck :
        public NActors::TEventPB<TEvAck, NSo::MetricQueue::TEvAck, EvAck> {
        
        TEvAck() = default;
        explicit TEvAck(const NDqProto::TMessageTransportMeta& transportMeta) {
            *Record.MutableTransportMeta() = transportMeta;
        }
    };

    struct TEvGetNextBatch :
        public NActors::TEventPB<TEvGetNextBatch, NSo::MetricQueue::TEvGetNextBatch, EvGetNextBatch> {
    };

    struct TEvMetricsBatch :
        public NActors::TEventPB<TEvMetricsBatch, NSo::MetricQueue::TEvMetricsBatch, EvMetricsBatch> {

        TEvMetricsBatch() = default;
        TEvMetricsBatch(std::vector<NSo::MetricQueue::TMetric> metrics, bool noMoreMetrics, ui64 downloadedBytes, const NDqProto::TMessageTransportMeta& transportMeta) {
            Record.MutableMetrics()->Assign(
                metrics.begin(), 
                metrics.end());
            Record.SetNoMoreMetrics(noMoreMetrics);
            Record.SetDownloadedBytes(downloadedBytes);
            *Record.MutableTransportMeta() = transportMeta;
        }
    };

    struct TEvMetricsReadError:
        public NActors::TEventPB<TEvMetricsReadError, NSo::MetricQueue::TEvMetricsReadError, EvMetricsReadError> {
        
        TEvMetricsReadError() = default;
        TEvMetricsReadError(const TString& issues, const NDqProto::TMessageTransportMeta& transportMeta) {
            Record.SetIssues(issues);
            *Record.MutableTransportMeta() = transportMeta;
        }
    };

    struct TEvConsumerFinished :
        public NActors::TEventPB<TEvConsumerFinished, NSo::MetricQueue::TEvConsumerFinished, EvConsumerFinished> {

        TEvConsumerFinished() = default;
        explicit TEvConsumerFinished(const NDqProto::TMessageTransportMeta& transportMeta) {
            *Record.MutableTransportMeta() = transportMeta;
        }
    };

    struct TEvPointsCountBatch : public NActors::TEventLocal<TEvPointsCountBatch, EvPointsCountBatch> {
        NSo::TGetPointsCountResponse Response;
        ui64 RequestId;
        TEvPointsCountBatch(const NSo::TGetPointsCountResponse& response, ui64 requestId)
            : Response(response)
            , RequestId(requestId)
        {}
    };

    struct TEvNewDataBatch: public NActors::TEventLocal<TEvNewDataBatch, EvNewDataBatch> {
        NSo::TGetDataResponse Response;
        ui64 RequestId;
        TEvNewDataBatch(NSo::TGetDataResponse&& response, ui64 requestId)
            : Response(std::move(response))
            , RequestId(requestId)
        {}
    };

    struct TEvRetryDataRequest: public NActors::TEventLocal<TEvRetryDataRequest, EvRetryDataRequest> {
        ui64 RequestId;
        explicit TEvRetryDataRequest(ui64 requestId)
            : RequestId(requestId)
        {}
    };
};

} // namespace NYql::NDq
