from grafanalib.core import (
    BarGauge,
    ConstantInput,
    DataSourceInput,
    GaugePanel,
    Heatmap,
    HeatmapColor,
    RowPanel,
    Stat,
    Threshold,
    YAxis,
)
from grafanalib import formatunits as UNITS
from scalgrafanalib import (
    layout,
    Dashboard,
    PieChart,
    Tooltip,
    Target,
    TimeSeries
)

up = Stat(
    title="Up",
    dataSource="${DS_PROMETHEUS}",
    reduceCalc="last",
    targets=[Target(
        expr='sum(up{namespace="${namespace}", job="${job}"})',
    )],
    thresholds=[
        Threshold("red", 0, 0.0),
        Threshold("green", 1, 1.0),
    ],
)

httpRequests = Stat(
    title="Http requests",
    dataSource="${DS_PROMETHEUS}",
    decimals=0,
    format=UNITS.SHORT,
    reduceCalc="last",
    targets=[Target(
        expr='sum(increase(http_requests_total{namespace="${namespace}", job="${job}"}[$__rate_interval]))',  # noqa: E501
    )],
    thresholds=[
        Threshold("green", 0, 0.0),
    ],
)

successRate = GaugePanel(
    title="Success rate",
    dataSource="${DS_PROMETHEUS}",
    calc="lastNotNull",
    format=UNITS.PERCENT_FORMAT,
    min=0,
    max=100,
    targets=[Target(
        expr="\n".join([
            'sum(rate(http_requests_total{namespace="${namespace}", job="${job}", code=~"2.."}[$__rate_interval])) * 100',  # noqa: E501
            "   /",
            'sum(rate(http_requests_total{namespace="${namespace}", job="${job}"}[$__rate_interval]))',  # noqa: E501
        ]),
        instant=True,
        legendFormat="Success rate",
    )],
    thresholds=[
        Threshold("red",    0, 0.0),
        Threshold("orange", 1, 80.0),
        Threshold("green",  2, 90.0),
    ],
)

dataIngestionRate = Stat(
    title="Injection Data Rate",
    description=(
        "Rate of data ingested : cumulative amount of data created (>0) or "
        "deleted (<0) per second."
    ),
    dataSource="${DS_PROMETHEUS}",
    colorMode="background",
    decimals=1,
    format="binBps",
    reduceCalc="mean",
    targets=[Target(
        expr='-sum(deriv(cloud_server_data_disk_available{namespace="${namespace}", job="${job}"}[$__rate_interval]))',  # noqa: E501
    )],
    thresholds=[
        Threshold("dark-purple", 0, 0.0),
    ],
)

objectIngestionRate = Stat(
    title="Injection Rate",
    description=(
        "Rate of object ingestion : cumulative count of object created (>0) "
        "or deleted (<0) per second."
    ),
    dataSource="${DS_PROMETHEUS}",
    colorMode="background",
    decimals=1,
    format="O/s",
    reduceCalc="mean",
    targets=[Target(
        expr='sum(deriv(cloud_server_number_of_objects{namespace="${namespace}", job="${job}"}[$__rate_interval]))',  # noqa: E501
    )],
    thresholds=[
        Threshold("dark-purple", 0, 0.0),
    ],
)

bucketsCounter = Stat(
    title="Buckets",
    description=(
        "Number of S3 buckets available in the cluster.\n"
        "This value is computed asynchronously, and update "
        "may be delayed up to 1h."
    ),
    dataSource="${DS_PROMETHEUS}",
    colorMode="value",
    format=UNITS.SHORT,
    noValue="-",
    reduceCalc="lastNotNull",
    targets=[Target(
        expr='sum(cloud_server_number_of_buckets{namespace="${namespace}",job="${reportJob}"})',  # noqa: E501
    )],
    thresholds=[
        Threshold("#808080", 0, 0.0),
        Threshold("blue", 1, 0.0),
    ],
)

objectsCounter = Stat(
    title="Objects",
    description=(
        "Number of S3 objects available in the cluster.\n"
        "This value is computed asynchronously, and update "
        "may be delayed up to 1h."
    ),
    dataSource="${DS_PROMETHEUS}",
    colorMode="value",
    format=UNITS.SHORT,
    noValue="-",
    reduceCalc="lastNotNull",
    targets=[Target(
        expr='sum(cloud_server_number_of_objects{namespace="${namespace}",job="${reportJob}"})',  # noqa: E501
    )],
    thresholds=[
        Threshold("#808080", 0, 0.0),
        Threshold("blue", 1, 0.0),
    ],
)

reporterUp = Stat(
    title="Up",
    description="Status of the reports-handler pod.",
    dataSource="${DS_PROMETHEUS}",
    reduceCalc="last",
    targets=[Target(
        expr='sum(up{namespace="${namespace}", job="${reportJob}"})',
    )],
    thresholds=[
        Threshold("red", 0, 0.0),
        Threshold("green", 1, 1.0),
    ],
)

lastReport = Stat(
    title="Last Report",
    description=(
        "Time elapsed since the last report, when object/bucket count was "
        "updated."
    ),
    dataSource="${DS_PROMETHEUS}",
    format=UNITS.SECONDS,
    noValue="-",
    reduceCalc="last",
    targets=[Target(
        expr="\n".join([
            'time()',
            '- max by() (cloud_server_last_report_timestamp{namespace="${namespace}", job="${reportJob}"})',  # noqa: E501
            '+ (cloud_server_last_report_timestamp{namespace="${namespace}", job="${reportJob}"}',  # noqa: E501
            '   - on() kube_cronjob_status_last_schedule_time{namespace="${namespace}", cronjob="${countItemsJob}"}',  # noqa: E501
            '   > 0 or vector(0))',
        ])
    )],
    thresholds=[
        Threshold("#808080", 0, 0.0),
        Threshold("green", 1, 0.0),
        Threshold("super-light-yellow", 2, 1800.),
        Threshold("orange", 3, 3600.),
        Threshold("red", 4, 3700.),
    ],
)


def http_status_panel(title, code):
    # type: (str, str) -> Stat
    return Stat(
        title=title,
        dataSource="${DS_PROMETHEUS}",
        colorMode="background",
        decimals=0,
        format=UNITS.SHORT,
        noValue="0",
        reduceCalc="lastNotNull",
        targets=[Target(
            expr='sum(increase(http_requests_total{namespace="${namespace}",job="${job}",code=' + code + "}[$__rate_interval]))",  # noqa: E501
        )],
        thresholds=[Threshold("semi-dark-blue", 0, 0.)],
    )


status200 = http_status_panel(title="Status 200", code='"200"')
status403 = http_status_panel(title="Status 4xx", code='~"4.."')
status5xx = http_status_panel(title="Status 5xx", code='~"5.."')

activeRequests = Stat(
    title="Active requests",
    dataSource="${DS_PROMETHEUS}",
    reduceCalc="lastNotNull",
    targets=[Target(
        expr='sum(http_active_requests{namespace="${namespace}", job="${job}"})',  # noqa: E501
    )],
    thresholds=[
        Threshold("green", 0, 0.0),
        Threshold("red", 1, 80.0),
    ],
)

oobDataIngestionRate = Stat(
    title="OOB Inject. Data Rate",
    description=(
        "Rate of data ingested out-of-band (OOB) : cumulative amount of OOB "
        "data created (>0) or deleted (<0) per second."
    ),
    dataSource="${DS_PROMETHEUS}",
    colorMode="background",
    decimals=1,
    format="binBps",
    reduceCalc="mean",
    targets=[Target(
        expr='sum(deriv(cloud_server_data_ingested{namespace="${namespace}", job="${job}"}[$__rate_interval]))',  # noqa: E501
    )],
    thresholds=[
        Threshold("purple", 0, 0.0),
    ],
)

oobObjectIngestionRate = Stat(
    title="OOB Inject. Rate",
    description=(
        "Rate of object ingested out-of-band (OOB) : cumulative count of OOB "
        "object created (>0) or deleted (<0) per second."
    ),
    dataSource="${DS_PROMETHEUS}",
    colorMode="background",
    decimals=1,
    format="O/s",
    reduceCalc="mean",
    targets=[Target(
        expr='sum(deriv(cloud_server_number_of_ingested_objects{namespace="${namespace}", job="${job}"}[$__rate_interval]))',  # noqa: E501
    )],
    thresholds=[
        Threshold("purple", 0, 0.0),
    ],
)

httpStatusCodes = TimeSeries(
    title="Http status code over time",
    dataSource="${DS_PROMETHEUS}",
    decimals=0,
    fillOpacity=30,
    lineInterpolation="smooth",
    unit=UNITS.SHORT,
    targets=[Target(
        expr='sum by (code) (increase(http_requests_total{namespace="${namespace}", job="${job}"}[$__rate_interval]))',  # noqa: E501
        legendFormat="{{code}}",
    )],
)


def http_aggregated_request_target(title, code):
    # type: (str, str) -> Target
    return Target(
        expr='sum(increase(http_requests_total{namespace="${namespace}", job="${job}", code=' + code + "}[$__rate_interval]))",  # noqa: E501
        legendFormat=title,
    )


def color_override(name, color):
    # type: (str, str) -> dict
    return {
        "matcher": {"id": "byName", "options": name},
        "properties": [{
            "id": "color",
            "value": {"fixedColor": color, "mode": "fixed"}
        }],
    }


httpAggregatedStatus = TimeSeries(
    title="Aggregated status over time",
    dataSource="${DS_PROMETHEUS}",
    decimals=0,
    fillOpacity=39,
    lineInterpolation="smooth",
    unit=UNITS.SHORT,
    scaleDistributionType="log",
    stacking={"mode": "normal", "group": "A"},
    targets=[
        http_aggregated_request_target(title="Success", code='~"2.."'),
        http_aggregated_request_target(title="User errors", code='~"4.."'),
        http_aggregated_request_target(title="System errors", code='~"5.."'),
    ],
    overrides=[
        color_override("Success", "dark-blue"),
        color_override("User errors", "semi-dark-orange"),
        color_override("System errors", "semi-dark-red"),
    ],
)


def average_latency_target(title, action=""):
    # type: (str, str) -> Target
    extra = ', action=' + action if action else ""
    return Target(
        expr="\n".join([
            'sum(rate(http_request_duration_seconds_sum{namespace="${namespace}", job="${job}"' + extra + "}[$__rate_interval]))",  # noqa: E501
            "   /",
            'sum(rate(http_request_duration_seconds_count{namespace="${namespace}", job="${job}"' + extra + "}[$__rate_interval]))",  # noqa: E501
        ]),
        legendFormat=title,
    )


averageLatencies = TimeSeries(
    title="Average latencies",
    dataSource="${DS_PROMETHEUS}",
    lineInterpolation="smooth",
    unit=UNITS.SECONDS,
    targets=[
        average_latency_target(title="Overall"),
        average_latency_target(
            title="Upload",
            action='~"objectPut|objectPutPart|objectCopy|objectPutCopyPart"'
        ),
        average_latency_target(title="Delete", action='"objectDelete"'),
        average_latency_target(title="Download", action='"objectGet"'),
        average_latency_target(
            title="Multi-delete", action='~"multiObjectDelete|multipartDelete"'
        ),
    ],
)

requestTime = Heatmap(
    title="Request time",
    dataSource="${DS_PROMETHEUS}",
    dataFormat="tsbuckets",
    maxDataPoints=25,
    tooltip=Tooltip(show=True, showHistogram=True),
    yAxis=YAxis(format=UNITS.DURATION_SECONDS),
    color=HeatmapColor(mode="opacity"),
    targets=[Target(
        expr='sum by(le) (increase(http_request_duration_seconds_bucket{namespace="${namespace}", job="${job}"}[$__interval]))',  # noqa: E501
        format="heatmap",
        legendFormat="{{ le }}",
    )],
)


def axisPlacement_override(name, mode):
    # type: (str, str) -> None
    return {
        "matcher": {"id": "byName", "options": name},
        "properties": [{
            "id": "custom.axisPlacement",
            "value": mode,
        }],
    }


bandWidth = TimeSeries(
    title="Bandwidth",
    dataSource="${DS_PROMETHEUS}",
    unit="binBps",
    targets=[
        Target(
            expr='sum(rate(http_response_size_bytes_sum{namespace="${namespace}", job="${job}"}[$__rate_interval]))',  # noqa: E501
            legendFormat="Out"
        ),
        Target(
            expr='sum(rate(http_request_size_bytes_sum{namespace="${namespace}", job="${job}"}[$__rate_interval]))',  # noqa: E501
            legendFormat="In"
        )
    ],
    thresholds=[
        Threshold("green", 0, 0.0),
        Threshold("red",   1, 80.0),
    ],
    overrides=[
        axisPlacement_override("Out", "right"),
    ],
)

uploadChunkSize = BarGauge(
    title="Upload chunk size",
    dataSource="${DS_PROMETHEUS}",
    calc="last",
    displayMode="gradient",
    format="bytes",
    max=None,
    min=None,
    # TODO: noValue=0,
    orientation="vertical",
    targets=[Target(
        expr='sum(increase(http_request_size_bytes{namespace="${namespace}",service="${job}"}[$__interval])) by (le)',  # noqa: E501
        format='heatmap',
        legendFormat='{{ le }}',
    )],
    thresholds=[
        Threshold("green", 0, 0.0),
    ]
)

downloadChunkSize = BarGauge(
    title="Download chunk size",
    dataSource="${DS_PROMETHEUS}",
    calc="last",
    displayMode="gradient",
    format="bytes",
    max=None,
    min=None,
    # TODO: noValue=0,
    orientation="vertical",
    targets=[Target(
        expr='sum(increase(http_request_size_bytes{namespace="${namespace}",service="${job}"}[$__interval])) by (le)',  # noqa: E501
        format='heatmap',
        legendFormat='{{ le }}',
    )],
    thresholds=[
        Threshold("green", 0, 0.0),
    ]
)


def top10_errors_by_bucket(title, code):
    # type: (str, str) -> PieChart
    return PieChart(
        title=title,
        dataSource="${DS_LOKI}",
        displayLabels=['name'],
        legendDisplayMode='table',
        legendPlacement='right',
        legendValues=['value'],
        pieType='donut',
        unit=UNITS.SHORT,
        targets=[Target(
            expr="\n".join([
                "topk(10, sum by(bucketName) (",
                '    count_over_time({namespace="${namespace}", pod=~"${pod}-.*"}',  # noqa: E501
                '                    | json | bucketName!="" and httpCode=' + code,  # noqa: E501
                "                    [$__interval])",
                "))",
            ]),
            legendFormat='{{bucketName}}',
        )],
    )


top10Error404ByBucket = top10_errors_by_bucket(
    title="404 : Top10 by Bucket", code='"404"'
)
top10Error500ByBucket = top10_errors_by_bucket(
    title="500 : Top10 by Bucket", code='"500"'
)
top10Error5xxByBucket = top10_errors_by_bucket(
    title="5xx : Top10 by Bucket", code='~"5.."'
)

dashboard = (
    Dashboard(
        title="CloudServer",
        editable=True,
        refresh="30s",
        tags=["CloudServer"],
        timezone="",
        inputs=[
            DataSourceInput(
                name="DS_PROMETHEUS",
                label="Prometheus",
                pluginId="prometheus",
                pluginName="Prometheus",
            ),
            DataSourceInput(
                name="DS_LOKI",
                label="Loki",
                pluginId="loki",
                pluginName="Loki"
            ),
            ConstantInput(
                name="namespace",
                label="namespace",
                description="Namespace associated with the Zenko instance",
                value="zenko",
            ),
            ConstantInput(
                name="job",
                label="job",
                description="Name of the Cloudserver job, used to filter only "
                            "the Cloudserver instances.",
                value="artesca-data-connector-s3api-metrics",
            ),
            ConstantInput(
                name="pod",
                label="pod",
                description="Prefix of the Cloudserver pod names, used to "
                            "filter only the Cloudserver instances.",
                value="artesca-data-connector-cloudserver",
            ),
            ConstantInput(
                name="reportJob",
                label="report job",
                description="Name of the Cloudserver Report job, used to "
                            "filter only the Report Handler instances.",
                value="artesca-data-ops-report-handler",
            ),
            ConstantInput(
                name="countItemsJob",
                label="count-items job",
                description="Name of the Count-Items cronjob, used to filter "
                            "only the Count-Items instances.",
                value="artesca-data-ops-count-items",
            )
        ],
        panels=layout.column([
            layout.row(
                [up, httpRequests, successRate, dataIngestionRate,
                 objectIngestionRate]
                + layout.resize([bucketsCounter], width=7)
                + layout.resize([reporterUp], width=2),
                height=4),
            layout.row(
                [status200]
                + layout.resize([status403, status5xx, activeRequests],
                                width=2)
                + [oobDataIngestionRate, oobObjectIngestionRate]
                + layout.resize([objectsCounter], width=7)
                + layout.resize([lastReport], width=2),
                height=4),
            RowPanel(title="Response codes"),
            layout.row([httpStatusCodes, httpAggregatedStatus], height=8),
            RowPanel(title="Latency"),
            layout.row([averageLatencies, requestTime], height=8),
            RowPanel(title="Data rate"),
            layout.row(layout.resize([bandWidth], width=12)
                       + [uploadChunkSize, downloadChunkSize],
                       height=8),
            RowPanel(title="Errors"),
            layout.row([
                top10Error404ByBucket,
                top10Error500ByBucket,
                top10Error5xxByBucket
            ], height=8),
        ]),
    )
    .auto_panel_ids()
    .verify_datasources()
)
