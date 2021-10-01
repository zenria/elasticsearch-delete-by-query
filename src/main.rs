use std::{collections::HashSet, time::Duration};

use indicatif::{ProgressBar, ProgressStyle};
use itertools::Itertools;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use structopt::StructOpt;
use tokio::time::sleep;

#[derive(StructOpt)]
struct Opt {
    #[structopt(short = "u", long = "url", default_value = "http://localhost:9200")]
    url: url::Url,
    #[structopt(short = "r", long = "requests-per-seconds", default_value = "100")]
    requests_per_second: i32,
    #[structopt(short = "i", long = "index", default_value = "*")]
    index: String,
    /// Scroll size parameter (batch size)
    #[structopt(short = "s", long = "scroll-size")]
    scroll_size: Option<u64>,
    /// JSON encoded query
    /// eg: {"range":{"lastIndexingDate":{"lte":"now-3y"}}}
    query: serde_json::Value,
}

#[derive(Serialize, Deserialize, Debug)]
struct TaskId(String);

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opt: Opt = Opt::from_args();
    let client = reqwest::ClientBuilder::new()
        .timeout(Duration::from_secs(60))
        .build()?;

    let bar = ProgressBar::new(1);
    bar.set_style(
        ProgressStyle::default_bar()
            .template("{spinner} [{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} {msg}")
            .progress_chars("##-"),
    );

    {
        let bar = bar.clone();
        tokio::spawn(async move {
            loop {
                bar.tick();
                sleep(Duration::from_millis(100)).await;
            }
        });
    }

    let mut deleted_total = 0;
    let mut hits = None;
    'retry: loop {
        bar.set_message("Sending delete by query...");
        let task_id = send_delete_by_query_task(&opt, &client).await?;
        bar.println(format!("Task ID: {}", task_id.0));
        bar.set_message("Waiting for task...");
        sleep(Duration::from_secs(2)).await;
        'status: loop {
            match get_task(&task_id, &opt, &client).await {
                Ok(response) => {
                    match hits {
                        Some(total) => {
                            // when ES has not yet really started the task, it will report a total if 0
                            // so let's update it if needed
                            if response.task.status.total > total {
                                hits = Some(response.task.status.total);
                                bar.set_length(response.task.status.total.max(0) as u64);
                            }
                        }
                        None => {
                            hits = Some(response.task.status.total);
                            bar.set_length(response.task.status.total.max(0) as u64);
                        }
                    }
                    if response.task.status.total > 0 {
                        bar.set_message("Delete in progress");
                    }
                    bar.set_position(deleted_total + response.task.status.deleted.max(0) as u64);
                    bar.tick();
                    match response.completed {
                        true => {
                            if let Some(response) = response.response {
                                deleted_total += response.status.deleted.max(0) as u64;
                                if response.failures.len() > 0 {
                                    bar.set_message("Error, will retry in 60s");

                                    bar.println(format!(
                                        "Failure detected: \n{}",
                                        response
                                            .failures
                                            .iter()
                                            .map(|f| (
                                                f.node.as_str(),
                                                f.index.as_str(),
                                                f.reason.reason.as_str()
                                            ))
                                            .collect::<HashSet<_>>()
                                            .iter()
                                            .map(|f| format!("({}, {}, {})", f.0, f.1, f.2))
                                            .join(", ")
                                    ));
                                    sleep(Duration::from_secs(60)).await;
                                    // let's retry
                                    break 'status;
                                }
                            } else {
                                bar.println(format!(
                                    "No 'response' field in completed task response: \n{}",
                                    serde_json::to_string_pretty(&response)?
                                ));
                            }
                            break 'retry;
                        }
                        false => {
                            // in progress, just wait
                            sleep(Duration::from_secs(10)).await;
                        }
                    }
                }
                Err(e) => {
                    bar.println(format!("Unable to get task: {}", e));
                    sleep(Duration::from_secs(5)).await;
                }
            }
        }
    }
    bar.set_message("Task completed without failures.");
    bar.finish_at_current_pos();

    Ok(())
}

async fn send_delete_by_query_task(opt: &Opt, client: &Client) -> anyhow::Result<TaskId> {
    let url = opt.url.join(&format!(
        "/{}/_delete_by_query?wait_for_completion=false&conflicts=proceed&requests_per_second={}{}",
        opt.index,
        opt.requests_per_second,
        opt.scroll_size
            .map_or("".to_string(), |s| format!("&scroll_size={}", s))
    ))?;
    let request = client
        .post(url)
        .json(&DeleteByQuery {
            query: opt.query.clone(),
        })
        .build()?;
    Ok(client
        .execute(request)
        .await?
        .error_for_status()?
        .json::<DeleteByQueryResponse>()
        .await?
        .task)
}

#[derive(Serialize)]
struct DeleteByQuery {
    query: serde_json::Value,
}

async fn get_task(task_id: &TaskId, opt: &Opt, client: &Client) -> anyhow::Result<GetTaskResponse> {
    let url = opt.url.join(&format!("/_tasks/{}", task_id.0))?;
    Ok(client
        .get(url)
        .send()
        .await?
        .error_for_status()?
        .json::<GetTaskResponse>()
        .await?)
}

#[derive(Serialize, Deserialize, Debug)]
struct DeleteByQueryResponse {
    task: TaskId,
}

#[derive(Serialize, Deserialize, Debug)]
struct GetTaskResponse {
    completed: bool,
    task: Task,
    response: Option<TaskResponse>,
}

#[derive(Serialize, Deserialize, Debug)]
struct Task {
    node: String,
    id: u64,
    r#type: String,
    action: String,
    status: TaskStatus,
    description: String,
    start_time_in_millis: u128,
    running_time_in_nanos: u128,
    cancellable: bool,
    headers: serde_json::Value,
}

#[derive(Serialize, Deserialize, Debug)]
struct TaskStatus {
    total: i64,
    updated: i64,
    created: i64,
    deleted: i64,
    batches: i64,
    version_conflicts: i64,
    noops: i64,
    retries: TaskRetries,
    throttled_millis: i64,
    requests_per_second: f64,
    throttled_until_millis: i64,
}

#[derive(Serialize, Deserialize, Debug)]
struct TaskRetries {
    bulk: i64,
    search: i64,
}

#[derive(Serialize, Deserialize, Debug)]
struct TaskResponse {
    #[serde(flatten)]
    status: TaskStatus,
    took: i64,
    timed_out: bool,
    throttled: String,
    throttled_until: String,
    failures: Vec<Failure>,
}
#[derive(Serialize, Deserialize, Debug)]
struct Failure {
    index: String,
    node: String,
    shard: i64,
    reason: Reason,
}
#[derive(Serialize, Deserialize, Debug)]
struct Reason {
    reason: String,
    r#type: String,
}
