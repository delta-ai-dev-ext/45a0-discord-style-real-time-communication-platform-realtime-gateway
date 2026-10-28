/** Kubernetes probes (docs/openapi_spec.yaml `/healthz`, `/readyz`). */

import { Elysia } from "elysia";
import type { HealthController } from "../controllers/health-controller";

export function createHealthRoutes(controller: HealthController) {
  return new Elysia({ name: "health-routes" })
    .get("/healthz", () => controller.liveness())
    .get("/readyz", ({ set }) => {
      const readiness = controller.readiness();
      set.status = readiness.httpStatus;
      return readiness.body;
    });
}
