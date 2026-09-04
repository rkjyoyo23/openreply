import { NextRequest, NextResponse } from "next/server";
import os from "node:os";
import { createDMWorker } from "@/lib/queue/dm-worker";
import { recordWorkerHeartbeat } from "@/lib/ops/worker-health";

/**
 * Serverless stand-in for the always-on DM worker.
  *
   * OpenReply's worker (worker/dm-worker.ts) is designed to run forever on a
    * dedicated host. This project has no such host — instead, an external
     * scheduler hits this endpoint roughly once every few minutes.
      * Each hit spins up a real BullMQ Worker, lets it drain whatever jobs are
       * ready during a bounded window, then shuts it down before the serverless
        * function's time limit. Comments arrive via the webhook as usual; this
         * endpoint only stands in for the process that would otherwise send the
          * DMs 24/7.
           *
            * Trade-off vs. a real always-on worker: a DM can wait up to one scheduler
             * interval before it goes out, instead of being sent within seconds.
              * Delayed jobs (rate-limit retries, follow-up messages) that become due
               * between hits also wait for the next hit.
                */

export const runtime = "nodejs";
export const dynamic = "force-dynamic";
// Hobby plan (with Fluid Compute) allows up to 300s; stay comfortably
  // under that, with buffer for cold-start and shutdown overhead.
export const maxDuration = 55;

const RUN_WINDOW_MS = 20_000;

// Most schedulers can send a custom Authorization header, but some simple
// free "visit this URL periodically" pingers can't. Accept the secret as a
// ?secret= query param too so this endpoint works with either kind.
function isAuthorized(request: NextRequest, cronSecret: string): boolean {
      const authHeader = request.headers.get("authorization");
      if (authHeader === `Bearer ${cronSecret}`) {
              return true;
      }

      const querySecret = request.nextUrl.searchParams.get("secret");
      return querySecret === cronSecret;
}

export async function GET(request: NextRequest) {
      const cronSecret = process.env.CRON_SECRET;

      if (!cronSecret || !isAuthorized(request, cronSecret)) {
              return NextResponse.json(
                  { success: false, error: "Unauthorized" },
                  { status: 401 }
                      );
      }

      const startedAt = new Date().toISOString();

      await recordWorkerHeartbeat({
              pid: process.pid,
              hostname: os.hostname(),
              startedAt,
      }).catch(() => {});

      const worker = createDMWorker();

      try {
              await new Promise((resolve) => setTimeout(resolve, RUN_WINDOW_MS));
      } finally {
              await worker.close().catch(() => {});
      }

      return NextResponse.json({
              success: true,
              startedAt,
              ranMs: RUN_WINDOW_MS,
              finishedAt: new Date().toISOString(),
      });
}

