"""Small in-memory role-based access control helpers."""

from collections.abc import Iterable

from app.core.errors import PermissionDeniedError
from app.model.entity import Permission, Role, User


class RBACAuthorizer:
    """Manage a minimal User -> Role -> Permission authorization graph.

    The registry is intentionally in-memory. A future persistence adapter can
    load the same model entities without changing the authorization checks.
    """

    def __init__(
        self,
        *,
        permissions: Iterable[Permission] = (),
        roles: Iterable[Role] = (),
        users: Iterable[User] = (),
    ) -> None:
        self._permissions: dict[str, Permission] = {}
        self._roles: dict[str, Role] = {}
        self._users: dict[str, User] = {}

        for permission in permissions:
            self.register_permission(permission)
        for role in roles:
            self.register_role(role)
        for user in users:
            self.register_user(user)

    def register_permission(self, permission: Permission) -> None:
        self._register(self._permissions, permission.name, permission, "permission")

    def register_role(self, role: Role) -> None:
        self._register(self._roles, role.name, role, "role")

    def register_user(self, user: User) -> None:
        self._register(self._users, user.user_id, user, "user")

    def grant_permission_to_role(
        self, role_name: str, permission_name: str
    ) -> None:
        role = self._roles.get(role_name)
        if role is None:
            raise ValueError(f"unknown role '{role_name}'")
        if permission_name not in self._permissions:
            raise ValueError(f"unknown permission '{permission_name}'")
        self._roles[role_name] = role.model_copy(
            update={
                "permissions": frozenset(
                    (*role.permissions, permission_name)
                )
            }
        )

    def assign_role_to_user(self, user_id: str, role_name: str) -> None:
        user = self._users.get(user_id)
        if user is None:
            raise ValueError(f"unknown user '{user_id}'")
        if role_name not in self._roles:
            raise ValueError(f"unknown role '{role_name}'")
        self._users[user_id] = user.model_copy(
            update={"roles": frozenset((*user.roles, role_name))}
        )

    def has_permission(self, user_id: str, permission_name: str) -> bool:
        user = self._users.get(user_id)
        if user is None or not user.is_active:
            return False
        if permission_name not in self._permissions:
            return False
        return any(
            permission_name in self._roles[role_name].permissions
            for role_name in user.roles
            if role_name in self._roles
        )

    def require_permission(self, user_id: str, permission_name: str) -> None:
        if not self.has_permission(user_id, permission_name):
            raise PermissionDeniedError(user_id, permission_name)

    @staticmethod
    def _register(
        registry: dict[str, object],
        key: str,
        value: object,
        kind: str,
    ) -> None:
        if key in registry:
            raise ValueError(f"{kind} '{key}' is already registered")
        registry[key] = value
