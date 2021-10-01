use std::time::Duration;

use indicatif::{ProgressBar, ProgressStyle};
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
            .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} {msg}")
            .progress_chars("##-"),
    );

    let mut deleted_total = 0;
    let mut hits = None;
    'retry: loop {
        let task_id = send_delete_by_query_task(&opt, &client).await?;
        bar.println(format!("Task ID: {}", task_id.0));
        bar.set_message("Waiting for task...");
        sleep(Duration::from_secs(2)).await;
        'status: loop {
            match get_task(&task_id, &opt, &client).await {
                Ok(response) => {
                    if hits.is_none() {
                        hits = Some(response.task.status.total);
                        bar.set_length(response.task.status.total);
                        bar.set_message("Deleting...");
                    }
                    bar.set_position(deleted_total + response.task.status.deleted);
                    bar.tick();
                    match response.completed {
                        true => {
                            if let Some(response) = response.response {
                                deleted_total += response.status.deleted;
                                if response.failures.len() > 0 {
                                    bar.set_message("Error, will retry in 60s");
                                    bar.println(format!(
                                        "Failure detected: \n{}",
                                        serde_json::to_string_pretty(&response.failures)?
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
        "/{}/_delete_by_query?wait_for_completion=false&conflicts=proceed&requests_per_second={}",
        opt.index, opt.requests_per_second
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
    total: u64,
    updated: u64,
    created: u64,
    deleted: u64,
    batches: u64,
    version_conflicts: u64,
    noops: u64,
    retries: TaskRetries,
    throttled_millis: u64,
    requests_per_second: f64,
    throttled_until_millis: u64,
}

#[derive(Serialize, Deserialize, Debug)]
struct TaskRetries {
    bulk: u64,
    search: u64,
}

#[derive(Serialize, Deserialize, Debug)]
struct TaskResponse {
    #[serde(flatten)]
    status: TaskStatus,
    took: u64,
    timed_out: bool,
    throttled: String,
    throttled_until: String,
    failures: Vec<serde_json::Value>,
}
