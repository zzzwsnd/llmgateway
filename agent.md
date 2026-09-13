# Agent 开发约定

本项目的详细架构和开发约定请参阅 [AGENTS.md](AGENTS.md)。

当前核心边界：

- `domain/models`：只负责下游模型供应商能力。
- `domain/integrations`：预留 Tools、MCP、Skills 等领域能力。
- `domain/sessions`、`domain/messages`、`domain/authorization`：领域业务模块。
- `application/chat`：直接会话和 Agent Loop 的用例编排。
- `application/runs`：一次执行过程的生命周期管理。
- `admin/models`：外部协议和管理契约的 Pydantic 模型。
- `infrastructure`：数据库、HTTP、认证、配置和观测等技术实现。
