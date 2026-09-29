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

## 评分组件维护窗口

升级评分组件前，运维可按 **模板（template_codes）/课程（project_codes）/班级（class_codes）** 圈选任务，创建维护窗口（`POST /api/compute/maintenance-windows`）。窗口依次经历：

1. **预告 announced**：只做公告，不影响任何领取；可携带计划的排空/强制/恢复时刻。
2. **排空 draining**：领取队列通过 SQL anti-join 排除命中任务——只影响"新领取"，提交、心跳、完成、失败重试和租约回收均不受影响；已被工作者持有的短任务自然结束。
3. **强制处理 enforcing**：到达 `enforce_at` 时按 `deadline_policy` 处理存量的排队/运行任务：
   - `cancel`：排队任务取消、仍占用工作者的长任务强制取消；
   - `requeue`：运行中任务释放工作者并回到队列（不改写优先级、不增加尝试次数），排队任务保持原位；已存在的取消请求仍会被兑现。
4. **恢复 recovered**：解除领取限制，任务按原有 `priority DESC, created_at ASC` 顺序和配额重新参与竞争。
5. **撤销 revoked**：随时可撤销（`POST .../{code}/revoke`）；若已强制取消过任务，仍处于取消态、未被人工另行处置的任务会自动重新排队，已被人工处理的不覆盖。

推进接口 `POST /api/compute/maintenance-windows/{code}/advance?to_stage=...` 与详情接口返回：

- `progress.matched_task_counts` / `frozen_queued_tasks`：当前进度；
- `progress.blocked_workers`：阻塞对象（任务、工作者、租约到期时间）和原因；
- `progress.processed_by_action` / `processed_total`：截止处理的逐任务统计；
- `progress.overlapping_windows`：重叠窗口。

幂等与合并：

- 阶段流转与截止处理逐任务落 `compute_maintenance_task_actions`（窗口×任务×阶段 UNIQUE），同一动作重复调用、或定时推进与人工推进并发，都不会重复干预；
- 到达计划时刻时，**领取动作会自动推进窗口**（无需外部调度器）；
- 重叠窗口按"更严格限制"合并：领取冻结取所有排空/强制窗口命中范围的**并集**；重叠既按筛选维度判定，也按实际命中任务集合判定（如课程窗口与班级窗口命中同一任务）。

