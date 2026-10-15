import { printBlue, printGreen, printMagenta, printRed, printYellow } from "./utils/colorOut.js";
import { hasSecretWriteToken, setRepoSecret } from "./utils/githubSecrets.js";
import { maskDisplayName, maskIdentifier, sanitizeForLog, summarizeResponse } from "./utils/safeLog.js";
import { sendNotify } from "./utils/notify.js";
import { close_api, delay, send, startService, waitForApi } from "./utils/utils.js";
import { writeUserinfoFile, parseUserinfoJson } from "./utils/userinfo.js";
import { beijingDateStr, isBeijingSunday } from "./utils/date.js";
import { checkinStateEnabled, loadDoneToday, saveDoneToday } from "./utils/checkinState.js";

async function main() {

  const USERINFO = process.env.USERINFO
  let needRefresh = false
  const userinfo = parseUserinfoJson(USERINFO)
  if (!userinfo) {
    throw new Error("未配置或 USERINFO 格式错误（应为 [{\"userid\":\"...\",\"token\":\"...\"}]）")
  }

  // 启动服务并等待就绪（避免冷启动竞态导致首个请求失败）
  const api = startService()
  try {
    await waitForApi()
  } catch (e) {
    close_api(api)
    throw e
  }

  // 日期/星期统一按北京时间计算（与运行环境时区无关，兼容 UTC 的 Actions 与任意时区的 Docker/本地）
  const now = new Date()
  const date = beijingDateStr(now)

  // 手动触发（WebUI「立即签到」/ scheduler.js once）时跳过当日已完成签到的账号；
  // 定时任务与 GitHub Actions 不设置该标记，保持完整执行。
  const skipDone = ['1', 'true', 'yes', 'on'].includes(String(process.env.CHECKIN_SKIP_DONE || '').trim().toLowerCase())
  const stateEnabled = checkinStateEnabled(skipDone)
  const doneSet = stateEnabled ? loadDoneToday(date) : new Set()
  let skippedCount = 0

  const errorMsg = {}
  // 通知结果收集（并行完成后按账号原始顺序填充）
  const notifyResults = []
  let hasError = false

  // 单账号完整签到流程：并行时各账号独立执行，账号内部保持原有请求节奏（VIP 领取间隔 30s）
  async function processAccount(user, index) {
    const tag = `[#${index + 1}/${userinfo.length}]`
    const accountErrors = {} // 该账号的错误集（键名含账号标识，账号之间互不冲突，可安全并行写入）
    try {
      // 手动触发：当日已完成签到（无任何错误）的账号直接跳过，不再请求酷狗 API
      if (skipDone && doneSet.has(String(user.userid))) {
        const safeUserId = maskIdentifier(user.userid)
        printYellow(`${tag} 账号 ${safeUserId} 今日已签到，跳过`)
        skippedCount++
        return {
          nickname: safeUserId, status: '跳过', listen: '今日已签到',
          vipClaim: '-', vipExpiry: '-', error: ''
        }
      }

      // 单账号异常隔离：任何一个账号的请求/解析出错，只记录该账号失败，不影响其余账号继续执行
      try {
        let headers = { 'cookie': 'token=' + user.token + '; userid=' + user.userid }
        const userDetail = await send(`/user/detail?timestrap=${Date.now()}`, "GET", headers)
        if (userDetail?.data?.nickname == null) {
          const safeUserId = maskIdentifier(user.userid)
          printRed(`${tag} token过期或账号不存在, userid: ${safeUserId}`)
          accountErrors[safeUserId] = {
            msg: `token过期或账号不存在, userid: ${safeUserId}`,
            data: summarizeResponse(userDetail)
          }
          return {
            nickname: safeUserId, status: '失败', listen: '账号不存在',
            vipClaim: '0/8', vipExpiry: '未知', error: 'token过期或账号不存在'
          }
        }
        const safeNickname = maskDisplayName(userDetail.data.nickname)
        printMagenta(`${tag} 账号 ${safeNickname} 开始领取VIP...`)

        // 周日刷新token
        if (isBeijingSunday(now)) {
          const refreshToken = await send(`/login/token?timestrap=${Date.now()}`, "POST", headers)
          if (refreshToken?.status == 1) {
            if (refreshToken?.data?.token !== user.token) {
              needRefresh = true
              printYellow(`${tag} 账号 ${safeNickname} 需要刷新token`)
              user.token = refreshToken.data.token
              // 用新 token 重建本次请求的 headers，使后续听歌/VIP 领取使用刷新后的凭证
              headers = { 'cookie': 'token=' + user.token + '; userid=' + user.userid }
            }
          }
        }

        // 开始听歌
        printYellow(`${tag} 开始听歌领取VIP...`)
        // 听歌获取vip
        const listen = await send(`/youth/listen/song?timestrap=${Date.now()}`, "GET", headers)

        let listenStatus = '未知'
        if (listen.status === 1) {
          printGreen(`${tag} 听歌领取成功`)
          listenStatus = '成功'
        } else if (listen.error_code === 130012) {
          printGreen(`${tag} 今日已领取`)
          listenStatus = '今日已领取'
        } else {
          accountErrors[`${safeNickname} listen`] = summarizeResponse(listen)
          printRed(`${tag} 听歌领取失败`)
          listenStatus = '失败'
        }

        printYellow(`${tag} 开始领取VIP...`)
        let claimCount = 0
        let claimTotal = 0
        for (let i = 1; i <= 8; i++) {
          const ad = await send(`/youth/vip?timestrap=${Date.now()}`, "GET", headers)
          claimTotal = i
          if (ad.status === 1) {
            printGreen(`${tag} 第${i}次领取成功`)
            claimCount++
            if (i != 8) {
              await delay(30 * 1000)
            }
          } else if (ad.error_code === 30002) {
            printGreen(`${tag} 今天次数已用光`)
            break
          } else {
            printRed(`${tag} 第${i}次领取失败`)
            accountErrors[`${safeNickname} ad`] = summarizeResponse(ad)
            break
          }
        }

        let vipExpiry = '未知'
        const vip_details = await send(`/user/vip/detail?timestrap=${Date.now()}`, "GET", headers)
        if (vip_details.status === 1 && Array.isArray(vip_details.data?.busi_vip) && vip_details.data.busi_vip.length > 0) {
          vipExpiry = vip_details.data.busi_vip[0].vip_end_time
          printBlue(`${tag} 今天是：${date}`)
          printBlue(`${tag} VIP到期时间：${vipExpiry}\n`)
        } else {
          printRed(`${tag} 获取失败\n`)
          accountErrors[`${safeNickname} vip_details`] = summarizeResponse(vip_details)
        }

        // 全程无错（含"今日已领取"与"次数已用光"）即视为当日已完成签到，写入状态供手动触发跳过
        if (Object.keys(accountErrors).length === 0 && stateEnabled) {
          doneSet.add(String(user.userid))
        }

        return {
          nickname: safeNickname,
          status: listenStatus === '失败' || claimCount === 0 ? '部分失败' : '成功',
          listen: listenStatus,
          vipClaim: `${claimCount}/${claimTotal}`,
          vipExpiry,
          error: ''
        }
      } catch (err) {
        const safeUserId = maskIdentifier(user.userid || '未知')
        printRed(`${tag} 账号 ${safeUserId} 处理异常：${err && err.message ? err.message : String(err)}`)
        accountErrors[safeUserId] = { msg: '处理异常', error: err && err.message ? err.message : String(err) }
        return {
          nickname: safeUserId, status: '失败', listen: '异常',
          vipClaim: '0/8', vipExpiry: '未知',
          error: err && err.message ? err.message : String(err)
        }
      }
    } finally {
      // 各账号错误统一合并到全局（键名含账号标识互不冲突），保证"异常信息如下"输出与原有格式一致
      Object.assign(errorMsg, accountErrors)
    }
  }

  try {
    // 并发数（账号级并行）：默认不限制——有几个账号就几路全部并行；
    // CHECKIN_CONCURRENCY 可选限流：设 N = 最多 N 路，设 1 = 逐个串行（与旧版行为一致）
    const rawConcurrency = String(process.env.CHECKIN_CONCURRENCY || '').trim().toLowerCase()
    let CONCURRENCY
    if (!rawConcurrency || rawConcurrency === 'max' || rawConcurrency === '0') {
      CONCURRENCY = userinfo.length
    } else {
      const n = parseInt(rawConcurrency, 10)
      CONCURRENCY = Math.min(Math.max(1, n || 1), Math.max(1, userinfo.length))
    }
    if (userinfo.length > 1) {
      const source = (!rawConcurrency || rawConcurrency === 'max' || rawConcurrency === '0')
        ? '全部账号同时并行'
        : `限流 ${CONCURRENCY} 路（CHECKIN_CONCURRENCY，1 为逐个串行）`
      printYellow(`多账号并行签到：${CONCURRENCY} 个账号同时进行（${source}）`)
    }

    // 工作池：CONCURRENCY 个 worker 从队列取账号执行；results 始终按账号原始顺序排列
    const results = new Array(userinfo.length)
    let cursor = 0
    const worker = async () => {
      while (cursor < userinfo.length) {
        const index = cursor++
        results[index] = await processAccount(userinfo[index], index)
      }
    }
    await Promise.all(Array.from({ length: Math.min(CONCURRENCY, userinfo.length) }, worker))

    notifyResults.push(...results)
    hasError = Object.keys(errorMsg).length > 0
  } finally {
    close_api(api)
  }

  // 持久化当日已完成签到的账号（跨天自动失效），供手动触发时跳过
  if (stateEnabled) {
    if (saveDoneToday(date, doneSet)) {
      printYellow(`今日已完成签到的账号已记录（${doneSet.size} 个），手动触发时将自动跳过`)
    }
  }

  // 刷新后的凭据持久化：Docker 部署写入 USERINFO_FILE 文件（挂载卷），
  // Actions 部署写入 GitHub Secret <USERINFO>（使用完整 userinfo 数组，保留所有用户包括过期账号）
  let secretError = null
  if (needRefresh) {
    const userinfoJSON = JSON.stringify(userinfo)
    // 优先写本地凭据文件（Docker 模式），写失败时降级走 PAT
    let savedToFile = false
    if (process.env.USERINFO_FILE) {
      savedToFile = writeUserinfoFile(userinfoJSON)
    }
    if (savedToFile) {
      printGreen("USERINFO 文件 token 刷新成功")
    } else if (hasSecretWriteToken()) {
      try {
        setRepoSecret("USERINFO", userinfoJSON)
        printGreen("secret <USERINFO> token刷新成功")
      } catch (error) {
        printRed("token刷新失败")
        console.dir(sanitizeForLog({ message: error.message }), { depth: null })
        secretError = new Error("secret <USERINFO> token刷新失败")
      }
    } else {
      printYellow("存在账号需要刷新token，但是未配置PAT，未刷新token最多两个月后过期")
    }
  }

  // 构建通知内容（放在 secret 更新之后、错误抛出之前，确保始终执行）
  const skipCount = notifyResults.filter(r => r.status === '跳过').length
  const allSkipped = skippedCount > 0 && skippedCount === notifyResults.length

  // 手动触发且全部账号均已签到：不重复发送通知，避免冗余打扰
  if (allSkipped) {
    printGreen(`当日（${date}）所有账号均已签到，本次共跳过 ${skipCount} 个账号，无需发送通知`)
    if (Object.keys(errorMsg).length > 0) {
      printRed("异常信息如下:")
      console.dir(sanitizeForLog(errorMsg), { depth: null })
      throw new Error("领取异常")
    }
    if (secretError) {
      throw secretError
    }
    return
  }

  const title = `酷狗签到${hasError ? '异常' : '成功'} ${date}`
  let content = `📅 日期: ${date}\n`
  content += `📊 账号数: ${notifyResults.length}\n`
  const successCount = notifyResults.filter(r => r.status === '成功').length
  const failCount = notifyResults.length - successCount - skipCount
  content += `✅ 成功: ${successCount}  ⏭ 跳过: ${skipCount}  ❌ 失败: ${failCount}\n`

  for (const r of notifyResults) {
    content += `\n【${r.nickname}】\n`
    content += `  🎵 听歌领取: ${r.listen}\n`
    if (r.status === '跳过') {
      content += `  ⏭ 当日已完成签到，本次跳过\n`
      continue
    }
    content += `  🎁 VIP领取: ${r.vipClaim} 次\n`
    content += `  ⏰ VIP到期: ${r.vipExpiry}\n`
    if (r.error) {
      content += `  ⚠️ 错误: ${r.error}\n`
    }
  }

  // 发送通知（确保即使 secret 更新失败也能发出）
  try {
    await sendNotify(title, content)
  } catch (e) {
    printYellow(`通知发送异常: ${e.message}`)
  }

  if (Object.keys(errorMsg).length > 0) {
    printRed("异常信息如下:")
    console.dir(sanitizeForLog(errorMsg), { depth: null })
    throw new Error("领取异常")
  }

  if (secretError) {
    throw secretError
  }

}

// 以 exitCode 标记结果、等事件循环自然排空后退出：
// 避免 Windows 下 process.exit() 与子进程 stdio 管道清理竞态触发 libuv 断言崩溃；
// api 子进程已被 close_api 终止，管道关闭后进程会正常退出。
main().then(() => { process.exitCode = 0 }).catch(e => { console.error(e); process.exitCode = 1 })
