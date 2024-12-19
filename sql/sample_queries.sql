SELECT node.id
FROM read_json('/tmp/stats_exports/*/agg_stats/*.json')
limit 10;

-- Views, reads, reading time and earnings per story
SELECT node.title                                                                 AS title,
       node.totalStats.views                                                      AS views,
       node.totalStats.reads                                                      AS reads,
       node.readingTime,
       CAST(CONCAT(node.earnings.total.units, '.',
                   LEFT(CAST(node.earnings.total.nanos AS string), 2)) AS DOUBLE) AS earnings
FROM
    read_json('/tmp/stats_exports/*/agg_stats/*.json')
ORDER BY earnings DESC;

-- Earnings per month per story
SELECT a.node.title AS title, e.month AS month, e.total_earnings AS earn
FROM (SELECT d.id                                          AS id,
             STRFTIME(MAKE_TIMESTAMP(CAST(d.daily_earning.periodStartedAt AS BIGINT) * 1000),
                      '%Y-%m')                             AS month,
             ROUND(SUM(d.daily_earning.amount / 100.0), 2) AS total_earnings
      FROM (SELECT p.post.id AS id, UNNEST(p.post.earnings.dailyEarnings) AS daily_earning
            FROM (SELECT UNNEST(data.post) AS post
                  FROM
                      read_json('/tmp/stats_exports/*/post_events/*.json')) p) d
      GROUP BY id,
               month) e
         JOIN read_json('/tmp/stats_exports/*/agg_stats/*.json') a ON a.node.id = e.id
ORDER BY earn DESC;

-- Earnings per day per story
SELECT a.node.title AS title, e.date AS date, e.total_earnings AS earn
FROM (SELECT d.id                                          AS id,
             STRFTIME(MAKE_TIMESTAMP(CAST(d.daily_earning.periodStartedAt AS BIGINT) * 1000),
                      '%Y-%m-%d')                          AS date,
             ROUND(SUM(d.daily_earning.amount / 100.0), 2) AS total_earnings
      FROM (SELECT p.post.id AS id, UNNEST(p.post.earnings.dailyEarnings) AS daily_earning
            FROM (SELECT UNNEST(data.post) AS post
                  FROM
                      read_json('/tmp/stats_exports/*/post_events/*.json')) p) d
      GROUP BY id,
               date) e
         JOIN read_json('/tmp/stats_exports/*/agg_stats/*.json') a ON a.node.id = e.id
ORDER BY earn DESC;

-- Earnings per interaction per story
SELECT id,
       STRFTIME(MAKE_TIMESTAMP(CAST(earnings.periodStartedAt AS BIGINT) * 1000),
                '%Y-%m-%d')                                      AS date,
       earnings.amount                                           AS amount,
       stats.readersThatReadCount                                AS reads,
       stats.readersThatViewedCount                              AS views,
       stats.readersThatClappedCount                             AS claps,
       stats.readersThatRepliedCount                             AS replies,
       stats.readersThatHighlightedCount                         AS highlights,
       stats.readersThatInitiallyFollowedAuthorFromThisPostCount AS follows
FROM (SELECT d.id               AS id,
             d.stats            AS stats,
             UNNEST(d.earnings) AS earnings
      FROM (SELECT t.post.id                                   AS id,
                   t.post.earnings.dailyEarnings               AS earnings,
                   UNNEST(t.post.postStatsDailyBundle.buckets) AS stats
            FROM (SELECT UNNEST(data.post) AS post
                  FROM read_json('/tmp/stats_exports/*/post_earnings_breakdown/*.json')) t) d
      WHERE earnings NOT NULL AND stats.membershipType = 'MEMBER')
WHERE earnings.periodStartedAt = stats.dayStartsAt
ORDER BY amount DESC;

-- Linear regression of interactions with amount
SELECT REGR_SLOPE(earnings.amount, stats.readersThatReadCount)                                AS slope_read,
       REGR_SLOPE(earnings.amount, stats.readersThatViewedCount)                              AS slope_view,
       REGR_SLOPE(earnings.amount, stats.readersThatClappedCount)                             AS slope_clap,
       REGR_SLOPE(earnings.amount, stats.readersThatRepliedCount)                             AS slope_reply,
       REGR_SLOPE(earnings.amount, stats.readersThatHighlightedCount)                         AS slope_highlight,
       REGR_SLOPE(earnings.amount, stats.readersThatInitiallyFollowedAuthorFromThisPostCount) AS slope_follow,
       REGR_INTERCEPT(earnings.amount, stats.readersThatReadCount)                            AS intercept
FROM (SELECT d.id               AS id,
             d.stats            AS stats,
             UNNEST(d.earnings) AS earnings
      FROM (SELECT t.post.id                                   AS id,
                   t.post.earnings.dailyEarnings               AS earnings,
                   UNNEST(t.post.postStatsDailyBundle.buckets) AS stats
            FROM (SELECT UNNEST(data.post) AS post
                  FROM read_json('/tmp/stats_exports/*/post_earnings_breakdown/*.json')) t) d
      WHERE earnings NOT NULL AND stats.membershipType = 'MEMBER')
WHERE earnings.periodStartedAt = stats.dayStartsAt;
