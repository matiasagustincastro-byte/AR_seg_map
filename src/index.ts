import cors from "@fastify/cors";
import Fastify from "fastify";
import { config } from "./config.js";
import { registerRoutes } from "./api/routes.js";
import { startScheduler } from "./scheduler.js";

const app = Fastify({
  logger: true
});

app.addContentTypeParser("application/x-www-form-urlencoded", { parseAs: "string" }, (_request, _body, done) => {
  done(null, {});
});

await app.register(cors, {
  origin: true
});
await app.register(registerRoutes);

startScheduler();

await app.listen({
  port: config.PORT,
  host: "0.0.0.0"
});
