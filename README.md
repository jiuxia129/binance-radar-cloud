# Binance Radar Cloud 云端异动监控

电脑关机也能推送：把监控部署到 GitHub Actions（境外服务器），每 5 分钟自动检测
币安现货 + 合约全市场 5 分钟窗口暴涨（>=5%），命中即向 QQ 邮箱推送邮件。

- 现货：官方公共镜像 `data-api.binance.vision`（国内直连不可达也不影响，云端在境外）
- 合约：官方 `fapi.binance.com`，云端直连，**无需代理**
- 电脑关机、断网均不影响推送

## 部署步骤（一次性配置，约 5 分钟）

### 1. 创建仓库
1. 打开 https://github.com/new
2. Repository name 填 `binance-radar-cloud`，选 **Public**（公开，公开仓库的 GitHub Actions 免费无限额度，支持每 5 分钟高频监控不耗尽配额；脚本不含任何密钥，敏感信息全部走 Secrets 加密存储）
3. 点击 Create repository

### 2. 上传文件
把本目录下这些文件上传到仓库根目录（页面点 Add file -> Upload files 即可）：
- `monitor.py`
- `.github/workflows/monitor.yml`（保持该目录结构）

### 3. 配置 Secrets（邮箱推送密钥）
仓库页面 -> **Settings** -> **Secrets and variables** -> **Actions** -> **New repository secret**，添加 3 个：

| Name | Value |
|------|-------|
| `MAIL_USER` | 2963025181@qq.com |
| `MAIL_PASS` | QQ 邮箱 SMTP 授权码 |
| `MAIL_TO` | 2963025181@qq.com, 第二个邮箱@xx.com, 第三个邮箱@xx.com |

> 授权码获取：QQ 邮箱设置 -> 账户 -> POP3/IMAP/SMTP 服务 -> 开启后生成授权码
> **多邮箱同时推送**：`MAIL_TO` 支持填多个收件邮箱（最多 3 个），用英文逗号分隔，例如 `2963025181@qq.com,abc@163.com,def@foxmail.com`，每个邮箱都会收到推送邮件。

### 4. 启用定时任务
仓库页面 -> **Actions** -> 左侧选中 **Binance Radar Cloud Monitor** -> 点击 **Enable workflow**。
定时任务会在整点每 5 分钟自动运行（`*/5 * * * *`），约 10 分钟后开始正常推送。
也可点右侧 **Run workflow** 手动触发一次立即验证。

### 5. 验证
手动触发后查看 Actions 运行日志，出现 `推送成功` 或 `本轮完成: spot=OK fut=OK` 即正常。
首次运行后 5 分钟内可查收 QQ 邮箱测试邮件。

## 触发条件
- 现货 / 合约任意币种，最近 5 分钟窗口涨幅 >= 5%
- 同币种 5 分钟冷却防刷屏；每轮最多 3 封

## 停止监控
仓库 -> Actions -> 右上角三个点 -> **Disable workflow**，或直接删掉 `.github/workflows/monitor.yml`。
