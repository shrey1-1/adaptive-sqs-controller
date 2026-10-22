# Real-SQS replication (Phase 6.1)

`src/replication.py` runs the same seeded jobs and policies through a real SQS queue and through the simulator, then compares them. One run per policy: it tests whether the simulator predicts reality, not whether one policy beats another.

## What it does

- Sends 40 jobs (the seeded `mixed` workload) to your queue, all at once, tagged with a fresh run id.
- Four worker threads receive, "process" each job by sleeping for its duration, heartbeat where the policy says so, and delete on success.
- Crashes are injected from the same `FailureModel.sample_for(seed, job, receive count)` draws as the simulator (8% per attempt): a crashing worker stops part-way and abandons the message, so SQS must redeliver it.
- The same jobs and policies are simulated, and `replication_summary.csv` lists real against simulated duplicates, recovery, latency, attempts, heartbeat extensions and makespan.
- Policies: `fixed_short`, `fixed_heartbeat`, `adaptive_p90`, `adaptive_heartbeat_dynamic`. Visibility timeouts are rounded up to whole seconds, as SQS requires, in both the real run and its simulated twin.

## Safety

- It never creates, purges or deletes a queue.
- It refuses to start unless the queue reports zero messages.
- It processes only messages carrying its own run id; any other message is released immediately with visibility 0.
- At the end it deletes its own leftover copies.

## Cost

Roughly 2,000–3,000 requests for all four policies (sends, long polls, visibility changes, deletes). That is far inside the SQS free tier of 1 million requests a month; beyond the free tier it would cost well under one US cent. Wall-clock time is about 4–6 minutes.

## Checklist

1. In the AWS console, create a **Standard** queue named `adaptive-visibility-test` (default settings; no dead-letter queue needed).
2. Create an IAM user or role with the policy in [iam-policy.json](iam-policy.json), after replacing `REGION` and `ACCOUNT_ID`. It allows only the five calls the harness makes, on that one queue.
3. Make credentials available through the normal AWS chain (for example `aws configure`, or environment variables). Never put them in the repository.
4. Install boto3: `python -m pip install boto3`.
5. Dry run offline first (no AWS calls):
   ```
   python -m src.replication --fake --time-scale 0.2
   ```
6. Real run:
   ```
   python -m src.replication --queue-url https://sqs.REGION.amazonaws.com/ACCOUNT_ID/adaptive-visibility-test --region REGION
   ```
7. Results land in `results/aws_replication/`. Delete the queue in the console afterwards if you no longer need it.

## Alternative: AWS CloudShell (no access keys)

CloudShell is a browser terminal that is already signed in to your account, so steps 2–3 are not needed:

```bash
aws sqs create-queue --queue-name adaptive-visibility-test
git clone https://github.com/shrey1-1/adaptive-sqs-controller.git
cd adaptive-sqs-controller
pip3 install -q -r requirements.txt          # not --user: CloudShell runs Python in a virtual environment
QURL=$(aws sqs get-queue-url --queue-name adaptive-visibility-test --query QueueUrl --output text)
python3 -m src.replication --queue-url "$QURL" --region "$AWS_REGION" --no-open
zip -r aws_real_run.zip results/aws_replication   # then Actions → Download file
aws sqs delete-queue --queue-url "$QURL"
```

The recorded real run (22 Oct 2026) was made this way; its results are in `results/aws_replication/` and summarised in the README (Phase 6.1).

## What to expect and how to read it

The fake dry run matches its simulated twin almost exactly, because the fake queue follows the simulator's rules; that only shows the harness is correct. Against real SQS, expect differences from network latency (tens of milliseconds per call), long-poll timing, thread scheduling, and SQS's own behaviour (for example whether a stale receipt handle can still delete a message). Agreement on duplicates and attempts within a job or two, and on recovery within about a second, would support the simulator; larger gaps are findings to report, not failures to hide.

## Known differences from the simulator

- The worker does not know a job's type until it reads the body, so it receives with a 2 s lease and immediately sets the policy's lease: one extra API call per receive, and a brief window the simulator does not have.
- Crashes are simulated inside a live process (the thread abandons the message); a real crash would also lose the process.
- The harness counts its own API calls; SQS billing counts each long poll, including empty ones.
