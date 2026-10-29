# Prism Hub Desktop

该目录是 Prism Hub 的 Tauri 2 桌面应用。它启动同包的 Go `prism-sidecar`，随后在
应用窗口中加载本地控制界面。关闭窗口只隐藏到托盘；从托盘选择“退出”或应用 Quit 才停止 sidecar。桌面应用是唯一的用户生命周期入口，并通过单实例 IPC 保证第二次启动只聚焦已有窗口，不会创建第二个 Hub 或插件运行时。

## 构建

在目标操作系统上执行：

```bash
npm run build:prod
```

构建脚本从 `config/deployments.local.properties` 读取线上环境的 CN/Global endpoint，将其编译进当前 sidecar 和 Desktop。该文件不进入安装包；运行时不提供环境切换。构建脚本只将当前平台的 Go Hub 编译到 `resources/bin/`，再由 Tauri 随安装包分发。Plugin、Plugin SDK 和 Node Runtime 均由 Hub 在用户明确安装 Plugin 时按需下载，不进入 DMG、MSI、DEB 等 Desktop 安装包。

本机快速回归只生成可运行的 macOS app、不生成 DMG 时可执行：

```bash
PRISM_SKIP_CODESIGN=1 PRISM_BUNDLES=app npm run build:prod
```

两个开关都必须显式提供，不改变默认发布构建的签名和完整 bundle 行为。

## 客户端更新

Desktop 使用 Tauri updater 从当前构建环境的
`/public/releases/current?channel=<environment>&format=tauri` 检查更新。发布构建必须配置：

- GitHub Environment Variable：`TAURI_UPDATER_PUBKEY`，由 `npx tauri signer generate` 生成的公钥。
- GitHub Environment Secrets：`TAURI_SIGNING_PRIVATE_KEY` 和可选的
  `TAURI_SIGNING_PRIVATE_KEY_PASSWORD`，与上述公钥配对的私钥。

工作流会保留 Tauri 生成的更新包和 `.sig` 签名，并将 URL、签名写入发布注册清单。客户端只在
签名验证成功后替换程序并重启。
