# Prism Hub Desktop

该目录是 Prism Hub 的 Tauri 2 桌面应用。它启动同包的 Go `prism-sidecar`，随后在
应用窗口中加载本地控制界面。关闭窗口只隐藏到托盘；从托盘选择“退出”或应用 Quit 才停止 sidecar。桌面应用是唯一的用户生命周期入口，并通过单实例 IPC 保证第二次启动只聚焦已有窗口，不会创建第二个 Hub 或插件运行时。

## 开发

`npm run dev` 会先按当前分支的线上配置重新编译 Go sidecar，再启动 Tauri
开发窗口：

```bash
npm ci
npm run dev
```

open-source 分支只支持线上服务，开发和打包都直接使用仓库中的
`config/deployments.local.properties`，不需要设置额外的 endpoint。

## 打包

本地 macOS 构建只生成可运行的 app，不签名、不生成 DMG：

```bash
PRISM_SKIP_CODESIGN=1 PRISM_BUNDLES=app npm run build
```

构建脚本从 `config/deployments.local.properties` 读取线上 CN/Global endpoint，
将其编译进当前 sidecar 和 Desktop。该文件不进入安装包；运行时不提供 endpoint 切换。
构建脚本只将当前平台的 Go Hub 编译到 `resources/bin/`，再由 Tauri 随安装包分发。
Plugin、Plugin SDK 和 Node Runtime 均由 Hub 在用户明确安装 Plugin 时按需下载，不进入
DMG、MSI、DEB 等 Desktop 安装包。输出位于 `src-tauri/target/release/bundle/` 下的
当前平台目录。

发布安装包的签名和公证由发布环境负责，不属于本地开发流程。
