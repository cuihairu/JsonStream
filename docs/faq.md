# 故障排查 / FAQ

仓库维护与文档站使用中的高频问题，全部以仓库实际配置为准（vitepress 配置、两个 workflow、pnpm 设置均给出处）。帧类型、错误码这类协议术语拿不准时，先对照[协议术语速查](/glossary)。

## 在线文档 404：Pages 对 URL 大小写敏感

现象：`https://cuihairu.github.io/jsonstream/design` 返回 404，但仓库里明明有 `docs/DESIGN.md`。

原因：GitHub Pages 的静态文件服务对路径大小写敏感。仓库名大小写混用的文档会带来大小写混用的 URL：

| 文件名 | 在线 URL | 说明 |
| --- | --- | --- |
| `docs/DESIGN.md` | `/DESIGN` | 全大写，写小写 `/design` 就是 404 |
| `docs/NOTES.md` | `/NOTES` | 同上 |
| `docs/design-notes.md` | `/design-notes` | 小写文件名自然是小写 URL |

做法：保持「URL 与源文件名逐字符一致」即可。VitePress 的死链检查只校验链接能否解析到文件，不校验大小写（大小写错误在本地构建时依然全绿），所以这是构建门禁抓不到的一类 404——新增页面时先确认文件名大小写，再写引用。本站所有页面 URL 见 [README 的按读者角色导航](https://github.com/cuihairu/jsonstream#readme)。

## 部署路径与 base 配置

项目站挂在子路径下：本仓库是项目级 Pages（`cuihairu.github.io/jsonstream/`），不是用户主站（`*.github.io`），所以站点部署在 `/jsonstream/` 前缀下。

`docs/.vitepress/config.mts` 从环境变量推导 base，一套配置两处通用：

- CI 里（`GITHUB_ACTIONS=true`）：从 `GITHUB_REPOSITORY` 取仓库名，非 `*.github.io` 仓库推 `/${仓库名小写}/` 作为 base；
- 本地 dev/preview：没有这些变量，base 为 `/`。

所以本地预览的地址是 `http://localhost:4173/`（根路径），线上是 `/jsonstream/` 前缀——直接把本地绝对路径链接原样拷进文章不会有问题（VitePress 会带上 base），但手工拼的站外引用要自己补前缀。

部署链（`.github/workflows/pages.yml`）：`pnpm build` → `upload-pages-artifact`（产物 `docs/.vitepress/dist`）→ `deploy-pages`。三处容易踩：

1. PR 只构建不部署：`deploy` job 有 `if: github.event_name != 'pull_request'`，所以改文档的 PR 绿了也不代表线上已更新——必须合入 main。
2. 部署串行：`concurrency.group = github-pages` 且 `cancel-in-progress: false`，连续推送会排队依次发布，不会互相打断留下半套产物；连续 push 后看到线上还是旧版，等队列排完即可。
3. Pages 源必须是 "GitHub Actions"：仓库 Settings → Pages 若选的是分支部署，upload/deploy 两步产物不会真正发布。

## 依赖版本与 pnpm 设置

文档站依赖极简：唯一 devDependency 是 `vitepress ^1.6.4`，Node 22、pnpm 12（CI 用 `pnpm/action-setup@v4` 钉 version 12；本地版本以 `pnpm --version` 为准）。两处最容易卡住新环境：

`pnpm install` 失败：构建脚本默认全拒。pnpm 12 起，依赖的 postinstall 脚本默认一律不跑且安装以失败退出。esbuild 的 postinstall 负责落位平台二进制，vite 构建必需——`pnpm-workspace.yaml` 里已显式放行：

```yaml
allowBuilds:
  esbuild: true
```

如果在新仓库/新分支上装完报 esbuild 相关错误，先检查这段放行是否还在。CI 里一律 `pnpm install --frozen-lockfile`，锁文件与 package.json 不同步会直接红，本地改依赖后记得 `pnpm install` 更新 `pnpm-lock.yaml` 一并提交。

本地构建/预览命令（`package.json` scripts）：

```bash
pnpm install        # 首次
pnpm dev            # 本地开发，http://localhost:5173
pnpm build          # 构建到 docs/.vitepress/dist，自带死链检查（见下）
pnpm preview        # 预览构建产物，http://localhost:4173
```

`pnpm build` 的死链检查即文档链接门禁：docs 内相对链接指到不存在的页面会直接构建失败（pages workflow 每次推送都在跑）。但它抓不到大小写错误的链接与站外 URL——这两类已由 `pnpm check:links`（`docs/.vitepress/check-links.mjs`，pages workflow 在 build 后自动跑）程序化补位：内链校验大小写与跨页 `#锚点`；外链做真实 HEAD 探测（只认 200/301/302，网络不可达时跳过不误报），因此外链失效或瞬时故障也可能让 pages 构建变红。

## fuzz / 集成测试怎么跑

集成测试没有单独开关：`integration_test.go` 与其余测试同在根包、无 build tag，普通 `go test ./...` 就会一起跑。日常全量命令是 CI 同款：

```bash
go test -race -count=1 ./...
```

`-count=1` 不是可省略的冗余：Go 的测试结果缓存可能让重跑直接输出 `(cached)` 而什么都不执行——只改文档/工作流的推送尤其容易命中。门禁要能证明自己跑过。

fuzz 分两层（`fuzz_test.go`，共 5 个靶）：

- 种子语料随常规 `go test ./...` 跑，无需任何参数；
- 真正的挖掘按靶单跑，CI（ci.yml 的 fuzz smoke 步骤）同款命令：

```bash
go test -run '^$' -fuzz '^FuzzReadFrame$' -fuzztime 20s .
# 其余靶：FuzzTransformInbound / FuzzRawPeer / FuzzDecodeMeta / FuzzHandshakeJSON
```

要点（与 ci.yml 的实现一致）：

- 靶名可用 `go test -list 'Fuzz.*' .` 发现，逐包扫而非只扫根包，避免靶挪包后被静默漏掉；
- `-fuzztime 20s` 是 CI 预算口径（本地深挖不受此限，README 记录的两个真 bug 是分钟级深挖撞出来的）；
- fuzz 不带 `-race`：race detector 下变异吞吐掉一个数量级，竞态由常规 `-race` 全量跑负责，两者分工不同；
- 崩溃语料会写进 `testdata/fuzz/<靶名>/`，提交回来就是一条永久回归用例（仓库里现有的 `testdata/fuzz/FuzzReadFrame/` 即历史真 bug 的沉淀）。

覆盖率门禁本地可复现（ci.yml coverage gate 步骤）：

```bash
go test -coverprofile=coverage.out -count=1 .
go tool cover -func=coverage.out | tail -1   # 库包语句覆盖须 100%
```

## codecov.yml 门禁含义

根目录 `codecov.yml` 只约束 Codecov 侧的状态检查；仓库真正的覆盖率门禁在 `ci.yml`（库包语句覆盖 100%，跌破即红）。两套口径不同（Codecov 按行、CI 按 `go tool cover` 语句），不互相替代。逐项含义：

| 配置 | 值 | 含义 |
| --- | --- | --- |
| `require_ci_to_pass` | `true` | CI 没全绿时 Codecov 状态不裁决，避免「CI 挂了但 Codecov 绿」的假信号 |
| `project.target` | `99%` | 全仓行覆盖目标。当前实测 ≈99.85%（badge 四舍五入显示 100%），定 100% 会下一次推送即红 |
| `project.range` | `95..100` | 容差带：目标未达但落在带内仍放行，跌破 95% 必红 |
| `patch.target` | `100%` | 新增/改动行必须全覆盖，与 CI 的语句 100% 门禁同口径，防新代码带豁免落地 |
| `ignore` | `examples`、`docs`、`assets` | 不参与覆盖率状态计算：examples 有独立的实测 100%（但不为覆盖率改形）、docs/assets 无 Go 覆盖率 |

为什么 CI 绿了 badge 却没更新：ci.yml 里 codecov 上传步骤是 `fail_ci_if_error: false`——上传失败（网络、配额、`CODECOV_TOKEN` 缺失/失效）只影响徽章数据，不红 CI，与覆盖率门禁刻意解耦。**token 缺失时的降级行为：上传步骤静默失败（exit 0），徽章保持旧值或显示 0%/横线，CI 依然全绿——不产生任何外发数据，不重试，不阻断流水线**。另外上传步骤带 `if: matrix.go == 'stable'`：每次 push main 都会上传，但只在 stable 腿跑一次（1.24 腿只跑兼容性测试，不重复出报告）。排查顺序：Actions 里看上传步骤日志（成功会有 "queued for processing" 与报告 URL）→ Codecov 页面看该 commit 的报告是否已处理。badge 显示为 0% 或横线通常是报告尚未处理完，等几分钟再刷。

## 其他

- v0.1.0 的 release 链接：tag 曾只打在本地、未推远端，GitHub 上没有 release 页面（链接会 404），README 当时改为纯文字引用。2026-09-28 tag 已推送并发布 release，README 已恢复[发布说明链接](https://github.com/cuihairu/jsonstream/releases/tag/v0.1.0)。
- 为什么改了 README/工作流，CI 的测试结果像没跑过：见上文 `-count=1` 一节——测试缓存命中时不执行任何测试，这是 Go 缓存语义而非 CI 偷懒。
- 想跑基准：CI 只做冒烟（`go test -bench . -benchtime 1x`），本地认真跑用 `go test -bench . -benchmem .`。
- 发现了 crash 或线上文档问题：crash 语料按上文 fuzz 一节提交回 `testdata/fuzz/`；文档问题直接改 `docs/` 对应源文件，pages workflow 会随 main 自动发布。
