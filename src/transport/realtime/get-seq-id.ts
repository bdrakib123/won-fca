/**
 * Fetches MQTT sync sequence ID from GraphQL and starts listenMqtt.
 * Handles retries and auto re-login via fca-config.json when session expires.
 */
import { tokensViaAPI, normalizeCookieHeaderString } from "../../core/auth";
import { parseAndCheckLogin, saveCookies } from "../../utils/client";
import formatMod from "../../utils/format";
import { get, jar as globalJar } from "../../utils/request";

const { getType } = formatMod;

type Logger = (text: string, type?: string) => void;

interface GetSeqDeps {
  listenMqtt: (defaultFuncs: Loose, api: Loose, ctx: Loose, globalCallback: Loose) => void;
  logger: Logger;
  emitAuth: (ctx: Loose, api: Loose, globalCallback: Loose, reason: string, detail?: string) => void;
}

// Try to auto-login using API and refresh web session.
function createGetSeqID(deps: GetSeqDeps) {
  const { listenMqtt, logger, emitAuth } = deps;

  return function getSeqID(
    defaultFuncs: Loose,
    api: Loose,
    ctx: Loose,
    globalCallback: Loose,
    form: Record<string, Loose>,
    retryCount = 0
  ) {
    const MAX_RETRIES = 3;
    const RETRY_DELAY = 2000;
    ctx.t_mqttCalled = false;

    return defaultFuncs
      .post("https://www.facebook.com/api/graphqlbatch/", ctx.jar, form)
      .then(parseAndCheckLogin(ctx, defaultFuncs))
      .then(async (resData: Loose) => {
        if (getType(resData) !== "Array") {
          logger(`getSeqID: Unexpected response type: ${getType(resData)}, value: ${JSON.stringify(resData).substring(0, 200)}`, "warn");

          if (resData && typeof resData === "object") {
            const errorMsg = resData.error || resData.message || "";
            if (/Not logged in|login|blocked|401|403|checkpoint/i.test(errorMsg)) {
              throw { error: "Not logged in", originalResponse: resData };
            }
          }

          throw { error: "Not logged in", originalResponse: resData };
        }
        if (!Array.isArray(resData) || !resData.length) return;
        const lastRes = resData[resData.length - 1];
        if (lastRes && lastRes.successful_results === 0) return;

        const syncSeqId = resData[0]?.o0?.data?.viewer?.message_threads?.sync_sequence_id;
        if (syncSeqId) {
          ctx.lastSeqId = syncSeqId;
          logger("mqtt getSeqID ok -> listenMqtt()", "info");
          listenMqtt(defaultFuncs, api, ctx, globalCallback);
        } else {
          throw { error: "getSeqId: no sync_sequence_id found." };
        }
      })
      .catch(async (err: Loose) => {
        const detail = err && err.detail && err.detail.message ? ` | detail=${err.detail.message}` : "";
        const msg = ((err && err.error) || (err && err.message) || String(err || "")) + detail;

        const isAuthError = /Not logged in|no sync_sequence_id found|blocked the login|401|403/i.test(msg);
        if (isAuthError) {
          if (retryCount < MAX_RETRIES) {
            const delay = RETRY_DELAY * (retryCount + 1);
            logger(`getSeqID: retry ${retryCount + 1}/${MAX_RETRIES} after ${delay}ms... (error: ${msg})`, "warn");
            await new Promise((resolve) => setTimeout(resolve, delay));

            if (retryCount === 0 && ctx.loggedIn) {
              try {
                logger("getSeqID: refreshing session before retry...", "info");
                await get("https://www.facebook.com/", ctx.jar, null, ctx.globalOptions, ctx).then(saveCookies(ctx.jar));
              } catch (refreshErr: Loose) {
                logger(`getSeqID: session refresh failed: ${refreshErr && refreshErr.message ? refreshErr.message : String(refreshErr)}`, "warn");
              }
            }

            return getSeqID(defaultFuncs, api, ctx, globalCallback, form, retryCount + 1);
          }

          /*
           * IMPORTANT:
           * MQTT must never perform its own email/password/API
           * re-login after authentication retries are exhausted.
           *
           * TESSA owns authentication recovery.
           * WonFCA only reports the authentication failure.
           */
          logger(
            `getSeqID: authentication retries exhausted; handing recovery to TESSA: ${msg}`,
            "error"
          );

          if (/blocked/i.test(msg)) {
            return emitAuth(ctx, api, globalCallback, "login_blocked", msg);
          }
          if (/Not logged in/i.test(msg)) {
            return emitAuth(ctx, api, globalCallback, "not_logged_in", msg);
          }
        }

        logger(`getSeqID error: ${msg}`, "error");
        return emitAuth(ctx, api, globalCallback, "auth_error", msg);
      });
  };
}

export default createGetSeqID;


