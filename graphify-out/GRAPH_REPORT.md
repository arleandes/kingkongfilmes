# Graph Report - kingkongfilmes  (2026-11-23)

## Corpus Check
- 1 files · ~72,339 words
- Verdict: corpus is large enough that graph structure adds value.
- Unclassified: 1 file(s) not represented in the graph (top: (none) 1)

## Summary
- 431 nodes · 947 edges · 19 communities (17 shown, 2 thin omitted)
- Extraction: 99% EXTRACTED · 1% INFERRED · 0% AMBIGUOUS · INFERRED: 10 edges (avg confidence: 0.85)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `989ad240`
- Run `git rev-parse HEAD` and compare to check if the graph is stale.
- Run `graphify update .` after code changes (no API cost).

## Community Hubs (Navigation)
- enviar_texto
- _processar_dm_interno
- normalizar_texto
- db_cursor
- horario_bahia_agora
- app.py
- marcar_tarefas_pessoais_concluidas
- agendar_lembrete
- _jid_para_zapi_phone
- _baixar_arquivo_drive_por_id
- metricool_resumo_posts_ou_reels
- _lagobada_listar_itens_desde
- route
- _atualizar_flag_compromisso
- painel_conversas
- listar_grupos_com_atividade_hoje
- painel_pedidos
- registrar_pedido_pendente
- debug_testar_edicao_imagem

## God Nodes (most connected - your core abstractions)
1. `_processar_dm_interno()` - 68 edges
2. `db_cursor()` - 41 edges
3. `normalizar_texto()` - 31 edges
4. `_processar_pedido_metricool_dm()` - 27 edges
5. `enviar_texto()` - 26 edges
6. `chamar_claude()` - 22 edges
7. `horario_bahia_agora()` - 20 edges
8. `registrar_mensagem_grupo()` - 19 edges
9. `_processar_repeticao_semanal_metricool()` - 17 edges
10. `_finalizar_processamento_grupo()` - 16 edges

## Surprising Connections (you probably didn't know these)
- `_processar_dm_interno()` --indirect_call--> `marcar_compromisso_concluido()`  [INFERRED]
  app.py → app.py  _Bridges community 13 → community 1_
- `processar_mensagem_grupo()` --indirect_call--> `_finalizar_processamento_grupo()`  [INFERRED]
  app.py → app.py  _Bridges community 1 → community 4_
- `adicionar_evento_tarefa()` --calls--> `db_cursor()`  [EXTRACTED]
  app.py → app.py  _Bridges community 3 → community 0_
- `_atualizar_flag_compromisso()` --calls--> `db_cursor()`  [EXTRACTED]
  app.py → app.py  _Bridges community 3 → community 13_
- `atualizar_pedido_mudanca()` --calls--> `db_cursor()`  [EXTRACTED]
  app.py → app.py  _Bridges community 3 → community 14_

## Import Cycles
- None detected.

## Communities (19 total, 2 thin omitted)

### Community 0 - "enviar_texto"
Cohesion: 0.06
Nodes (32): adicionar_evento_tarefa(), baixar_midia(), baixar_midia_evolution(), baixar_midia_zapi_url(), _buscar_ultima_arte(), _conferir_ultima_arte_da_conversa(), enviar_texto(), _enviar_texto_evolution() (+24 more)

### Community 1 - "_processar_dm_interno"
Cohesion: 0.05
Nodes (32): _aguardar_imagens_recentes_para_edicao(), already_processed(), _buscar_imagens_recentes_para_edicao(), chamar_claude(), corrigir_texto_dm(), _descrever_conteudo_arquivo_dm(), _detectar_participante_grupo(), _detectar_pedido_edicao_imagem() (+24 more)

### Community 2 - "normalizar_texto"
Cohesion: 0.05
Nodes (29): _detectar_tipo_conteudo_metricool(), _extrair_data_hora_pedido_metricool(), _extrair_horario_lote_metricool(), _extrair_legenda_metricool(), _extrair_mes_ano_referencia_lote(), _extrair_referencia_dia_do_nome_arquivo(), _formatar_aviso_revisao_metricool(), _formatar_ordem_midias() (+21 more)

### Community 3 - "db_cursor"
Cohesion: 0.05
Nodes (25): _agendar_avisos_compromisso(), buscar_pedido_pendente(), buscar_tarefa_pendente_por_cliente(), cobrar_tarefas_pessoais_pendentes(), criar_compromisso_agenda(), criar_tarefa_pessoal(), db_cursor(), _enviar_aviso_agenda() (+17 more)

### Community 4 - "horario_bahia_agora"
Cohesion: 0.06
Nodes (26): _bloco_assuntos_conhecidos_cliente(), _bloco_fatos_cliente_completo(), _bloco_proximos_dias_semana(), _bloco_regras_para_prompt(), buscar_mensagens_grupo_por_termos(), buscar_mensagens_recentes_grupo(), comparar_arte_com_pedido(), dentro_do_horario_comercial() (+18 more)

### Community 5 - "app.py"
Cohesion: 0.09
Nodes (4): _nav_painel_html(), _pedido_status_label(), _render_painel_conversas_html(), _render_painel_pedidos_html()

### Community 6 - "marcar_tarefas_pessoais_concluidas"
Cohesion: 0.11
Nodes (11): _aplicar_verificacao_jev(), _bate_por_palavras_chave(), marcar_tarefas_pessoais_concluidas(), _normalizar_texto_busca(), _numero_e_pessoal(), _obter_cliente_typesafe(), _palavras_significativas_busca(), _pede_todas_tarefas_pessoais() (+3 more)

### Community 7 - "agendar_lembrete"
Cohesion: 0.13
Nodes (12): agendar_cobranca_tripa(), agendar_lembrete(), disparar_primeiro_aviso(), agendar_lembrete_recorrente(), disparar(), agendar_nag(), checar_e_reenviar(), _calcular_periodo_agenda() (+4 more)

### Community 8 - "_jid_para_zapi_phone"
Cohesion: 0.15
Nodes (12): buscar_nome_grupo(), buscar_nome_grupo_evolution(), buscar_nome_grupo_zapi(), enviar_audio(), enviar_midia(), _enviar_midia_evolution(), _enviar_midia_zapi(), _enviar_texto_zapi() (+4 more)

### Community 9 - "_baixar_arquivo_drive_por_id"
Cohesion: 0.14
Nodes (8): _baixar_arquivo_drive_por_id(), _baixar_midia_de_link_metricool(), _baixar_midias_de_link_metricool(), _extrair_pasta_id_drive(), _garantir_pasta_drive(), _listar_midias_pasta_drive(), _obter_drive_service(), salvar_midia_drive()

### Community 10 - "metricool_resumo_posts_ou_reels"
Cohesion: 0.14
Nodes (8): metricool_headers(), metricool_identificar_marca(), metricool_listar_marcas(), _metricool_periodo(), metricool_responder_metricas(), metricool_resumo_posts_ou_reels(), metricool_seguidores(), _somar_metricas_posts()

### Community 11 - "_lagobada_listar_itens_desde"
Cohesion: 0.18
Nodes (7): _lagobada_autenticado(), _lagobada_item_agenda(), _lagobada_item_lembrete_avulso(), _lagobada_item_tarefa(), _lagobada_listar_itens_desde(), lagobada_sync_pull(), lagobada_sync_push()

### Community 12 - "route"
Cohesion: 0.20
Nodes (6): health(), midia_publica(), _normalizar_evento_zapi(), status_memoria(), webhook(), webhook_zapi()

### Community 13 - "_atualizar_flag_compromisso"
Cohesion: 0.22
Nodes (6): _atualizar_flag_compromisso(), _buscar_compromisso_por_referencia(), _cancelar_avisos_compromisso(), excluir_compromisso_agenda(), marcar_compromisso_cancelado(), marcar_compromisso_concluido()

### Community 14 - "painel_conversas"
Cohesion: 0.25
Nodes (4): atualizar_pedido_mudanca(), _checar_token_painel(), painel_conversas(), painel_pedidos_atualizar()

### Community 15 - "listar_grupos_com_atividade_hoje"
Cohesion: 0.29
Nodes (4): listar_grupos_com_atividade_hoje(), _mensagem_de_hoje(), _nome_parece_id_tecnico(), responder_atividade_geral_hoje()

### Community 16 - "painel_pedidos"
Cohesion: 0.33
Nodes (3): criar_pedido_mudanca(), listar_pedidos_mudanca(), painel_pedidos()

## Knowledge Gaps
- **2 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `_processar_dm_interno()` connect `_processar_dm_interno` to `enviar_texto`, `normalizar_texto`, `db_cursor`, `horario_bahia_agora`, `app.py`, `marcar_tarefas_pessoais_concluidas`, `agendar_lembrete`, `_jid_para_zapi_phone`, `_baixar_arquivo_drive_por_id`, `_atualizar_flag_compromisso`?**
  _High betweenness centrality (0.038) - this node is a cross-community bridge._
- **Are the 4 inferred relationships involving `_processar_dm_interno()` (e.g. with `excluir_compromisso_agenda()` and `marcar_compromisso_cancelado()`) actually correct?**
  _`_processar_dm_interno()` has 4 INFERRED edges - model-reasoned connections that need verification._
- **Should `enviar_texto` be split into smaller, more focused modules?**
  _Cohesion score 0.05779220779220779 - nodes in this community are weakly interconnected._
- **Why does `db_cursor()` connect `db_cursor` to `enviar_texto`, `_processar_dm_interno`, `horario_bahia_agora`, `app.py`, `marcar_tarefas_pessoais_concluidas`, `agendar_lembrete`, `_lagobada_listar_itens_desde`, `route`, `_atualizar_flag_compromisso`, `painel_conversas`, `listar_grupos_com_atividade_hoje`, `painel_pedidos`, `registrar_pedido_pendente`?**
  _High betweenness centrality (0.018) - this node is a cross-community bridge._
- **Should `_processar_dm_interno` be split into smaller, more focused modules?**
  _Cohesion score 0.05454545454545454 - nodes in this community are weakly interconnected._
- **Why does `enviar_texto()` connect `enviar_texto` to `_processar_dm_interno`, `normalizar_texto`, `db_cursor`, `horario_bahia_agora`, `app.py`, `marcar_tarefas_pessoais_concluidas`, `agendar_lembrete`, `_jid_para_zapi_phone`?**
  _High betweenness centrality (0.012) - this node is a cross-community bridge._
- **Should `normalizar_texto` be split into smaller, more focused modules?**
  _Cohesion score 0.05387205387205387 - nodes in this community are weakly interconnected._