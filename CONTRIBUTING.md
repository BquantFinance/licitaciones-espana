# Gobernanza de Contribución

Este repositorio utiliza un flujo guiado por issues y PRs para proteger la calidad del ETL en producción y sus invariantes de arquitectura.

## Política base

- `main` está protegida y debe recibir cambios mediante Pull Requests.
- No se debe hacer push directo a `main`.
- Toda Pull Request debe estar vinculada a una issue (por ejemplo: `Closes #123`).
- Las PR deben centrarse en un único objetivo (feature, fix o refactor) e incluir evidencia de validación.
- En este modelo de mantenedor único, la elegibilidad de merge depende de checks de CI obligatorios y no de aprobaciones humanas obligatorias.

## Flujo de contribución requerido

1. Crear una issue con el problema/contexto y el cambio propuesto.
2. Crear una rama desde `main`:
   - Nomenclatura recomendada: `feat/<tema-corto>`, `fix/<tema-corto>`, `chore/<tema-corto>`.
3. Implementar el cambio y añadir/actualizar pruebas.
4. Actualizar `CHANGELOG.md` si se altera comportamiento, contratos, esquema o modelo de datos.
5. Abrir una Pull Request:
   - Usar la plantilla de PR.
   - Referenciar la issue (`Closes #...`).
   - Incluir notas de verificación (tests/lint/comprobaciones manuales).
6. Integrar cuando los checks obligatorios estén en verde.

## Expectativas específicas de ETL

- Mantener los invariantes del microservicio definidos en `README.md`:
  - El ETL debe seguir siendo autocontenido.
  - PostgreSQL es la única dependencia externa en runtime.
  - Debe mantenerse la paridad funcional CLI/API al añadir capacidades.
- Mantener comportamiento de ingesta determinista e idempotente cuando sea viable.
- Para migraciones, cambios en scheduler o cambios semánticos de ingesta, incluir notas de rollback en la PR.

## Política de bypass para owner/admin

- El owner/admin puede aplicar bypass de protecciones de rama en casos excepcionales.
- El bypass se reserva para emergencias (incidentes, hotfix urgente o recuperación del repositorio).
- Después de un bypass:
  - Documentar el motivo en una issue o comentario de PR.
  - Abrir una PR/issue de seguimiento si hace falta limpieza o revisión posterior.

## Criterios de revisión

- Priorizar corrección, seguridad de datos y fiabilidad operativa.
- Solicitar cambios cuando falten invariantes de arquitectura, contratos de datos o evidencia de validación.
