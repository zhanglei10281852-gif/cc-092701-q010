# 职业教学任务运营服务

这是一个面向职业院校教务团队、授课教师和课程管理员的 Python 后端服务，用于管理课程任务模板、学员提交、执行队列、教师工作者、结果版本和运营干预。服务保留登录、角色权限、会话、审计和配额等基础能力，所有业务状态与审计事件写入本地 SQLite 数据库，适合在单个应用容器中离线运行。

## 运行环境

- Python 3.11
- SQLite 3（由 Python 标准库提供）
- FastAPI 与 Uvicorn

## 安装

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev]"
```

默认数据库位于 `./data/compute-operations.db`，可复制 `.env.example` 并设置 `TOWNSHIP_DATABASE_PATH` 指向其他本地路径。

## 初始化与启动

```bash
python -m app.cli init-db
python -m app.cli check-db
uvicorn app.main:app --host 0.0.0.0 --port 8432
```

健康检查：

```bash
curl -sS http://127.0.0.1:8432/api/system/health
```

课程任务运营接口使用 `/api/compute` 前缀，身份、角色、审计和系统接口分别位于 `/api/auth`、`/api/roles`、`/api/audit` 与 `/api/system`。

## 维护窗口（评分组件升级）

升级前可按模板、课程（`project_code`）或班级（`class_code`，任务提交时可选传入）划定维护窗口，接口前缀 `/api/compute/maintenance-windows`：

1. `POST /api/compute/maintenance-windows?actor=...` 创建窗口，指定排空开始 `drain_at`、截止时间 `deadline_at` 与截止策略 `deadline_policy`（`cancel` 或 `requeue`），窗口进入 `announced`（预告）。
2. `POST /{id}/advance?action=drain` 进入排空；`action=enforce` 在截止时强制处理；`action=recover` 恢复。不带 `action` 时按当前时间自动推进到期阶段。
3. 排空（`draining`）期间只有命中窗口的新领取被拦截，存量运行任务不受影响；未命中课程/模板/班级的任务正常领取。
4. 截止（`enforced`）时按策略处置命中存量：`cancel` 取消排队/运行/待响应取消的任务，`requeue` 释放运行任务并保持原优先级重新排队，排队任务原位保留。
5. `POST /{id}/revoke` 可在预告或排空阶段撤销窗口；强制处理后只能恢复。
6. 时间重叠的活动窗口自动合并：取最早排空/截止时间、取并集筛选、策略取更严格的 `cancel`，被吸收窗口标记为 `merged`。
7. `GET /{id}/progress` 返回阶段、命中任务分布（`matched_open_tasks`）、阻塞对象及原因（`blockers`，如占用工作者与租约到期时间）、下一步动作（`next_action`/`next_action_at`）和最近事件。

同一推进动作重复调用是幂等空操作（`noop: true`），每个窗口/任务/动作有唯一处置标记，不会重复取消或重复登记干预。恢复后任务保持原有 `priority`、`created_at` 与可用性时间，回到原排序与配额竞争中。


## 测试与编译检查

```bash
python -m pytest
python -m compileall -q app tests
```

本地冒烟命令：

```bash
python -m app.cli smoke
python -m app.cli compute-demo
```

## 目录结构

```text
app/compute/       任务模板、配额、提交、领取、回执和人工干预
app/api/            登录、角色、审计和系统管理接口
app/core/           时钟、安全、异常和分页能力
app/repositories/   SQLite 查询与事务封装
app/services/       身份、审计和后台任务服务
app/database.py     SQLite 连接、事务、表结构和权限初始化
tests/              领域、接口、调度和身份回归测试
tools/              本地维护脚本
```

## 数据一致性

SQLite 连接启用外键、WAL 和忙等待策略。提交、领取、回执和人工干预在即时事务中完成；租约、配额与结果版本使用可注入时钟，便于复现跨日和恢复边界。会话令牌只保存摘要，审计与人工干预记录不会写入明文密码或令牌。
