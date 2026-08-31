# Webhook instantaneo - KingKong Filmes
#
# Duas funcoes nesse servico:
#
# 1. GRUPOS DE CLIENTE: quando chega mensagem nova num grupo de cliente,
#    usa a API da Anthropic (Claude) pra entender a mensagem (texto, imagem
#    ou audio transcrito), responde o cliente na hora no tom combinado, e
#    se parecer um cliente chateado/urgente, avisa a equipe (Torres e Luan)
#    na hora.
#
# 2. LEMBRETES PESSOAIS: quando Torres ou Luan mandam uma mensagem direta
#    (DM) pro numero do robo pedindo um lembrete, agenda um aviso 10
#    minutos antes, e insiste a cada 30 minutos ate a pessoa responder
#    qualquer coisa naquele DM.
#
# Variaveis de ambiente necessarias (configurar no Railway, aba Variables
# deste servico): ANTHROPIC_API_KEY, OPENAI_API_KEY, EVOLUTION_BASE_URL,
# EVOLUTION_APIKEY, EVOLUTION_INSTANCE, TORRES_NUMBER, LUAN_NUMBER
#
# Observacao: este servico guarda lembretes pendentes em memoria (nao em
# banco de dados) - se reiniciar com um lembrete pendente, ele se perde.
# Rode este servico com UMA unica instancia (nao escale horizontalmente).

import os
import re
import json
import base64
import tempfile
import time
import threading
import unicodedata
from contextlib import contextmanager
from datetime import datetime, timezone, timedelta

from flask import Flask, request, jsonify
import requests
from apscheduler.schedulers.background import BackgroundScheduler

try:
    import psycopg2
    import psycopg2.extras
    import psycopg2.pool
except ImportError:
    psycopg2 = None

app = Flask(__name__)
scheduler = BackgroundScheduler(timezone="UTC")
scheduler.start()

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
ANTHROPIC_WORKSPACE_ID = os.environ.get("ANTHROPIC_WORKSPACE_ID", "")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
EVOLUTION_BASE_URL = os.environ.get("EVOLUTION_BASE_URL", "").rstrip("/")
EVOLUTION_APIKEY = os.environ.get("EVOLUTION_APIKEY", "")
EVOLUTION_INSTANCE = os.environ.get("EVOLUTION_INSTANCE", "")
TORRES_NUMBER = os.environ.get("TORRES_NUMBER", "5571999394216")
LUAN_NUMBER = os.environ.get("LUAN_NUMBER", "5571992200583")
TEAM_NUMBERS = [TORRES_NUMBER, LUAN_NUMBER]

# Outro agente de IA (do próprio time) que também atende nos grupos de cliente -
# quando ele falar num grupo, o robô nunca deve responder/entrar na conversa,
# igual já acontece com mensagens do Torres e do Luan.
OUTRO_AGENTE_IA_NUMBER = os.environ.get("OUTRO_AGENTE_IA_NUMBER", "5571984224897")
NUMEROS_SEM_AUTO_RESPOSTA_EM_GRUPO = [TORRES_NUMBER, LUAN_NUMBER, OUTRO_AGENTE_IA_NUMBER]

# Integracao com o Metricool (consulta de metricas e agendamento de post via link do
# Drive) - disponivel SOMENTE no privado de Torres/Luan, nunca pra clientes. O userId
# da conta ja e conhecido (nao e segredo), mas o userToken e uma credencial e deve ser
# configurado so como variavel de ambiente no Railway, nunca escrito no codigo.
METRICOOL_BASE_URL = "https://app.metricool.com/api"
METRICOOL_USER_ID = os.environ.get("METRICOOL_USER_ID", "4956967")
METRICOOL_USER_TOKEN = os.environ.get("METRICOOL_USER_TOKEN", "")

# Grupos monitorados: id -> {"nome": ..., "interno": True/False}
GRUPOS = {
    "120363409281934368@g.us": {"nome": "Terapia", "interno": False},
    "120363215853284263@g.us": {"nome": "Zurca", "interno": False},
    "120363425598150153@g.us": {"nome": "Dr. Fellipe Barbosa", "interno": False},
    "120363422131389631@g.us": {"nome": "Correria • Gestão 💎", "interno": True},
    "120363403421546688@g.us": {"nome": "Criações/Gravações • Tripa • Correria", "interno": True},
    "120363410170863535@g.us": {"nome": "Curso doutor", "interno": False},
    "120363412192937913@g.us": {"nome": "Trafego Diego", "interno": False},
    "120363427807470909@g.us": {"nome": "Novo Mix", "interno": False},
    "120363405735535012@g.us": {"nome": "Asas do Brasil", "interno": False},
    "120363428506649593@g.us": {"nome": "Olegario", "interno": False},
    "120363408409998519@g.us": {"nome": "João Vaqueiro", "interno": False},
    "120363204077711888@g.us": {"nome": "Latidos e miados", "interno": False},
    "120363312409734498@g.us": {"nome": "House and Co", "interno": False},
    "120363407172233170@g.us": {"nome": "Chicafé", "interno": False},
    "120363410147723558@g.us": {"nome": "Grupo lembrete (TESTE)", "interno": False},
}

TRIPA_DESIGNER_JID = "120363403421546688@g.us"

# --------------------------------------------------------------------------
# Banco de dados (memoria de tarefas persistente)
# --------------------------------------------------------------------------
#
# Configure a variavel de ambiente DATABASE_URL (o Railway preenche isso
# automaticamente quando voce adiciona um servico de PostgreSQL ao mesmo
# projeto e referencia a variavel no servico do webhook). Sem essa variavel
# configurada, o servico continua funcionando normalmente, so que sem
# memoria de tarefas persistente (usa fallback em memoria, que se perde a
# cada redeploy - o mesmo comportamento de antes desta funcionalidade).

DATABASE_URL = os.environ.get("DATABASE_URL", "")

# Nome do schema Postgres dedicado a essa aplicacao. Usamos um schema separado
# (em vez do schema "public") pra garantir isolamento total mesmo quando
# DATABASE_URL aponta pro mesmo banco Postgres usado pelo Evolution API - as
# tabelas do Evolution ficam no schema "public" delas, e as nossas ficam aqui
# dentro, sem nenhum risco de colisao de nomes ou de mexer nos dados deles.
DB_SCHEMA = os.environ.get("DB_SCHEMA", "correria_tarefas")

STATUS_PENDENTE = "PENDENTE"
STATUS_EM_EXECUCAO = "EM_EXECUCAO"
STATUS_AGUARDANDO_CORRECAO = "AGUARDANDO_CORRECAO"
STATUS_CONCLUIDO = "CONCLUIDO"

_db_pool = None


def get_db_pool():
    global _db_pool
    if _db_pool is None and DATABASE_URL and psycopg2:
        _db_pool = psycopg2.pool.ThreadedConnectionPool(
            1, 10, dsn=DATABASE_URL,
            options=f"-c search_path={DB_SCHEMA},public",
        )
    return _db_pool


@contextmanager
def db_cursor(commit=False):
    """Context manager que empresta uma conexao do pool, devolve um cursor (linhas como
    dict), e sempre devolve a conexao pro pool no final (commitando se commit=True, ou
    dando rollback se der excecao no meio do caminho)."""
    pool = get_db_pool()
    if not pool:
        raise RuntimeError("DATABASE_URL nao configurada ou psycopg2 indisponivel")
    conn = pool.getconn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            yield cur
        if commit:
            conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        pool.putconn(conn)


def init_db():
    """Cria as tabelas de tarefas se ainda nao existirem. Chamado uma vez quando o
    servico sobe. Se DATABASE_URL nao estiver configurada, so avisa no log e segue -
    o resto do servico funciona normalmente com o fallback em memoria."""
    if not DATABASE_URL:
        print("[init_db] DATABASE_URL nao configurada - memoria de tarefas persistente desativada (usando fallback em memoria)", flush=True)
        return
    if not psycopg2:
        print("[init_db] biblioteca psycopg2 nao instalada (adicione psycopg2-binary no requirements.txt) - memoria de tarefas persistente desativada", flush=True)
        return
    try:
        with db_cursor(commit=True) as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {DB_SCHEMA}")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS tarefas (
                    id SERIAL PRIMARY KEY,
                    cliente_nome TEXT NOT NULL,
                    grupo_jid TEXT,
                    tipo_peca TEXT,
                    descricao TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'PENDENTE',
                    criado_em TIMESTAMPTZ NOT NULL DEFAULT now(),
                    atualizado_em TIMESTAMPTZ NOT NULL DEFAULT now()
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS tarefas_eventos (
                    id SERIAL PRIMARY KEY,
                    tarefa_id INTEGER NOT NULL REFERENCES tarefas(id) ON DELETE CASCADE,
                    tipo_evento TEXT NOT NULL,
                    conteudo TEXT,
                    autor TEXT,
                    criado_em TIMESTAMPTZ NOT NULL DEFAULT now()
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_tarefas_cliente_status ON tarefas (cliente_nome, status)")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS regras_atendimento (
                    id SERIAL PRIMARY KEY,
                    autor TEXT,
                    texto TEXT NOT NULL,
                    cliente TEXT,
                    criado_em TIMESTAMPTZ NOT NULL DEFAULT now()
                )
            """)
            # migracao pra bancos que ja tinham essa tabela antes da coluna "cliente" existir
            # (regras especificas de um cliente, em vez de regras gerais pra todo atendimento).
            cur.execute("ALTER TABLE regras_atendimento ADD COLUMN IF NOT EXISTS cliente TEXT")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS fatos_memoria (
                    id SERIAL PRIMARY KEY,
                    autor TEXT,
                    texto TEXT NOT NULL,
                    criado_em TIMESTAMPTZ NOT NULL DEFAULT now()
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS mensagens_grupo (
                    id SERIAL PRIMARY KEY,
                    grupo_jid TEXT NOT NULL,
                    grupo_nome TEXT,
                    autor TEXT,
                    eh_equipe BOOLEAN NOT NULL DEFAULT false,
                    conteudo TEXT,
                    criado_em TIMESTAMPTZ NOT NULL DEFAULT now()
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_mensagens_grupo_jid_criado ON mensagens_grupo (grupo_jid, criado_em)")
        print(f"[init_db] banco de dados pronto (schema '{DB_SCHEMA}', tabelas tarefas/tarefas_eventos/regras_atendimento/fatos_memoria/mensagens_grupo)", flush=True)
    except Exception as e:
        print(f"[init_db] erro ao inicializar banco de dados: {e}", flush=True)


# Regras permanentes de atendimento que Torres/Luan definem no privado do bot mandando
# "regra: <instrucao>". Ficam guardadas (banco, com fallback em memoria) e sao incluidas
# em TODA resposta automatica de cliente dali pra frente, ate alguem decidir remover.
_regras_memoria = []
_regras_lock = threading.Lock()


def salvar_regra(autor, texto, cliente=None):
    """Salva uma regra de atendimento. "cliente" vazio/None = regra GERAL, aplicada em
    todo atendimento; preenchido = regra ESPECÍFICA daquele cliente (guardada com o nome
    canônico do grupo, pra bater certinho na hora de listar)."""
    if DATABASE_URL:
        try:
            with db_cursor(commit=True) as cur:
                cur.execute(
                    "INSERT INTO regras_atendimento (autor, texto, cliente) VALUES (%s, %s, %s)",
                    (autor, texto, cliente),
                )
            return
        except Exception as e:
            print(f"[salvar_regra] banco de dados falhou, usando fallback em memoria: {e}", flush=True)
    with _regras_lock:
        _regras_memoria.append({"autor": autor, "texto": texto, "cliente": cliente})


def listar_regras(cliente=None):
    """Devolve as regras GERAIS (valem pra todo atendimento) seguidas das regras
    ESPECÍFICAS do cliente informado, se houver - assim uma orientação dada só pro
    cliente X (ex: "nesse cliente, quando perguntarem isso, pode dizer que...") nunca
    vaza pra outro cliente."""
    if DATABASE_URL:
        try:
            with db_cursor() as cur:
                cur.execute("SELECT texto, cliente FROM regras_atendimento ORDER BY criado_em ASC")
                linhas = cur.fetchall()
        except Exception as e:
            print(f"[listar_regras] erro: {e}", flush=True)
            return []
    else:
        with _regras_lock:
            linhas = list(_regras_memoria)
    gerais = [r["texto"] for r in linhas if not r.get("cliente")]
    cliente_norm = normalizar_texto(cliente) if cliente else ""
    especificas = [
        r["texto"] for r in linhas
        if cliente_norm and r.get("cliente") and normalizar_texto(r["cliente"]) == cliente_norm
    ]
    return gerais + especificas


# Fatos soltos que Torres/Luan contam no privado (preferencias, informacoes de clientes,
# etc - ex: "Luan gosta da cor rosa") e que o bot deve conseguir recuperar depois quando
# perguntado, em vez de responder de forma generica/aleatoria. Mesmo padrao hibrido
# banco+memoria das regras de atendimento.
_fatos_memoria = []
_fatos_lock = threading.Lock()


def salvar_fato(autor, texto):
    if DATABASE_URL:
        try:
            with db_cursor(commit=True) as cur:
                cur.execute(
                    "INSERT INTO fatos_memoria (autor, texto) VALUES (%s, %s)",
                    (autor, texto),
                )
            return
        except Exception as e:
            print(f"[salvar_fato] banco de dados falhou, usando fallback em memoria: {e}", flush=True)
    with _fatos_lock:
        _fatos_memoria.append({"autor": autor, "texto": texto})


def listar_fatos():
    if DATABASE_URL:
        try:
            with db_cursor() as cur:
                cur.execute("SELECT texto FROM fatos_memoria ORDER BY criado_em ASC")
                return [r["texto"] for r in cur.fetchall()]
        except Exception as e:
            print(f"[listar_fatos] erro: {e}", flush=True)
            return []
    with _fatos_lock:
        return [r["texto"] for r in _fatos_memoria]


# Historico leve de mensagens por grupo (equipe e cliente), pra Torres/Luan poderem
# perguntar no privado "o que rolou no grupo tal" sem precisar abrir o grupo. Fallback em
# memoria guarda so as ultimas mensagens por grupo, pra nao crescer sem limite.
_mensagens_grupo_memoria = {}  # grupo_jid -> lista de mensagens (mais recente por ultimo)
_mensagens_grupo_lock = threading.Lock()
_MENSAGENS_MEMORIA_MAX_POR_GRUPO = 100


def registrar_mensagem_grupo(grupo_jid, grupo_nome, autor, conteudo, eh_equipe):
    if DATABASE_URL:
        try:
            with db_cursor(commit=True) as cur:
                cur.execute(
                    "INSERT INTO mensagens_grupo (grupo_jid, grupo_nome, autor, eh_equipe, conteudo) "
                    "VALUES (%s, %s, %s, %s, %s)",
                    (grupo_jid, grupo_nome, autor, eh_equipe, conteudo),
                )
            return
        except Exception as e:
            print(f"[registrar_mensagem_grupo] banco de dados falhou, usando fallback em memoria: {e}", flush=True)
    with _mensagens_grupo_lock:
        fila = _mensagens_grupo_memoria.setdefault(grupo_jid, [])
        fila.append({
            "autor": autor, "conteudo": conteudo, "eh_equipe": eh_equipe,
            "criado_em": horario_bahia_agora().isoformat(),
        })
        if len(fila) > _MENSAGENS_MEMORIA_MAX_POR_GRUPO:
            del fila[0]


def buscar_mensagens_recentes_grupo(grupo_jid, limite=30):
    if DATABASE_URL:
        try:
            with db_cursor() as cur:
                cur.execute(
                    "SELECT autor, eh_equipe, conteudo, criado_em FROM mensagens_grupo "
                    "WHERE grupo_jid = %s ORDER BY criado_em DESC LIMIT %s",
                    (grupo_jid, limite),
                )
                return list(reversed(cur.fetchall()))
        except Exception as e:
            print(f"[buscar_mensagens_recentes_grupo] erro: {e}", flush=True)
            return []
    with _mensagens_grupo_lock:
        return list(_mensagens_grupo_memoria.get(grupo_jid, []))[-limite:]


def identificar_grupo_mencionado(texto):
    """Procura, no texto de uma mensagem de DM, o nome de algum grupo de cliente
    conhecido (comparacao sem acento/case, por substring) - pra Torres/Luan poderem
    perguntar sobre um grupo pelo nome sem precisar citar o JID. O grupo Tripa (interno)
    tambem pode ser perguntado assim, ja que o historico dele tambem fica registrado."""
    texto_norm = normalizar_texto(texto)
    if "tripa" in texto_norm:
        return TRIPA_DESIGNER_JID
    melhor = None
    for jid, info in GRUPOS.items():
        if info.get("interno"):
            continue
        nome_norm = normalizar_texto(info["nome"])
        if nome_norm and nome_norm in texto_norm:
            if not melhor or len(nome_norm) > len(normalizar_texto(GRUPOS[melhor]["nome"])):
                melhor = jid
    return melhor


def identificar_grupos_candidatos(texto):
    """Parecido com identificar_grupo_mencionado, mas devolve TODOS os grupos de cliente cujo
    nome aparece mencionado no texto (nao so o "melhor" match) - usado quando precisamos saber
    se uma mencao e ambigua (bate com mais de um cliente) antes de agir, em vez de escolher um
    silenciosamente. Ex: se um dia existirem "Terapia Beach" e "Terapia Eventos", mencionar so
    "Terapia" deve gerar uma pergunta de confirmacao, nunca uma escolha arbitraria."""
    texto_norm = normalizar_texto(texto)
    candidatos = []
    for jid, info in GRUPOS.items():
        if info.get("interno"):
            continue
        nome_norm = normalizar_texto(info["nome"])
        bateu = bool(nome_norm) and nome_norm in texto_norm
        if not bateu:
            for palavra in nome_norm.split():
                if len(palavra) >= 4 and palavra not in _STOPWORDS_NOME_CLIENTE and palavra in texto_norm:
                    bateu = True
                    break
        if bateu:
            candidatos.append(jid)
    return candidatos


def _mensagem_de_hoje(mensagem_criado_em, inicio_dia):
    """Compara o timestamp (string ISO, do fallback em memoria) de uma mensagem registrada
    com o inicio do dia atual (horario de Bahia), pra saber se ela e de hoje."""
    try:
        quando = datetime.fromisoformat(mensagem_criado_em)
    except Exception:
        return False
    return quando >= inicio_dia


def listar_grupos_com_atividade_hoje():
    """Retorna {grupo_jid: {"nome": ..., "mensagens": [...]}} pra todo grupo que teve pelo
    menos uma mensagem registrada hoje (horario de Bahia) - grupos de cliente e o grupo Tripa,
    mas NUNCA os DMs privados de Torres/Luan (esses nao contam como "grupo"). Usado pra
    responder perguntas do tipo "quais grupos tiveram atividade hoje"."""
    inicio_dia = horario_bahia_agora().replace(hour=0, minute=0, second=0, microsecond=0)
    grupos_ativos = {}
    if DATABASE_URL:
        try:
            with db_cursor() as cur:
                cur.execute(
                    "SELECT grupo_jid, grupo_nome, autor, eh_equipe, conteudo FROM mensagens_grupo "
                    "WHERE criado_em >= %s AND grupo_jid NOT LIKE 'dm_%%' ORDER BY grupo_jid, criado_em",
                    (inicio_dia,),
                )
                for row in cur.fetchall():
                    info = grupos_ativos.setdefault(row["grupo_jid"], {"nome": row["grupo_nome"], "mensagens": []})
                    info["mensagens"].append({
                        "autor": row["autor"], "conteudo": row["conteudo"], "eh_equipe": row["eh_equipe"],
                    })
            return grupos_ativos
        except Exception as e:
            print(f"[listar_grupos_com_atividade_hoje] erro no banco, usando fallback em memoria: {e}", flush=True)
    with _mensagens_grupo_lock:
        for grupo_jid, mensagens in _mensagens_grupo_memoria.items():
            if grupo_jid.startswith("dm_"):
                continue
            mensagens_hoje = [m for m in mensagens if _mensagem_de_hoje(m.get("criado_em", ""), inicio_dia)]
            if mensagens_hoje:
                nome = GRUPOS.get(grupo_jid, {}).get("nome", grupo_jid)
                grupos_ativos[grupo_jid] = {"nome": nome, "mensagens": mensagens_hoje}
    return grupos_ativos


def criar_tarefa(cliente_nome, grupo_jid, tipo_peca, descricao, autor="cliente"):
    """Cria uma tarefa nova no banco (pedido original) e registra o primeiro evento no
    historico. Devolve o id da tarefa criada, ou None se o banco nao estiver disponivel
    ou der erro (nesse caso quem chamou deve usar o fallback em memoria)."""
    try:
        with db_cursor(commit=True) as cur:
            cur.execute(
                "INSERT INTO tarefas (cliente_nome, grupo_jid, tipo_peca, descricao, status) "
                "VALUES (%s, %s, %s, %s, %s) RETURNING id",
                (cliente_nome, grupo_jid, tipo_peca, descricao, STATUS_PENDENTE),
            )
            tarefa_id = cur.fetchone()["id"]
            cur.execute(
                "INSERT INTO tarefas_eventos (tarefa_id, tipo_evento, conteudo, autor) "
                "VALUES (%s, %s, %s, %s)",
                (tarefa_id, "pedido_original", descricao, autor),
            )
        return tarefa_id
    except Exception as e:
        print(f"[criar_tarefa] erro: {e}", flush=True)
        return None


def buscar_tarefa_pendente_por_cliente(cliente_nome):
    """Pega a tarefa mais antiga daquele cliente que ainda nao foi concluida (PENDENTE,
    EM_EXECUCAO ou AGUARDANDO_CORRECAO). Devolve um dict com os campos da tarefa, ou None
    se nao tiver nenhuma tarefa aberta ou o banco nao estiver disponivel."""
    try:
        with db_cursor() as cur:
            cur.execute(
                "SELECT * FROM tarefas WHERE cliente_nome = %s AND status != %s "
                "ORDER BY criado_em ASC LIMIT 1",
                (cliente_nome, STATUS_CONCLUIDO),
            )
            return cur.fetchone()
    except Exception as e:
        print(f"[buscar_tarefa_pendente_por_cliente] erro: {e}", flush=True)
        return None


def adicionar_evento_tarefa(tarefa_id, tipo_evento, conteudo, autor="sistema", novo_status=None):
    """Registra um evento no historico da tarefa (alteracao, correcao, entrega, aprovacao,
    conclusao...) e, se informado, atualiza o status da tarefa junto."""
    if not tarefa_id:
        return
    try:
        with db_cursor(commit=True) as cur:
            cur.execute(
                "INSERT INTO tarefas_eventos (tarefa_id, tipo_evento, conteudo, autor) "
                "VALUES (%s, %s, %s, %s)",
                (tarefa_id, tipo_evento, conteudo, autor),
            )
            if novo_status:
                cur.execute(
                    "UPDATE tarefas SET status = %s, atualizado_em = now() WHERE id = %s",
                    (novo_status, tarefa_id),
                )
    except Exception as e:
        print(f"[adicionar_evento_tarefa] erro: {e}", flush=True)


def listar_tarefas_pendentes(cliente_nome=None):
    """Lista tarefas com status != CONCLUIDO, opcionalmente filtrando por cliente."""
    try:
        with db_cursor() as cur:
            if cliente_nome:
                cur.execute(
                    "SELECT * FROM tarefas WHERE cliente_nome = %s AND status != %s ORDER BY criado_em ASC",
                    (cliente_nome, STATUS_CONCLUIDO),
                )
            else:
                cur.execute(
                    "SELECT * FROM tarefas WHERE status != %s ORDER BY criado_em ASC",
                    (STATUS_CONCLUIDO,),
                )
            return cur.fetchall()
    except Exception as e:
        print(f"[listar_tarefas_pendentes] erro: {e}", flush=True)
        return []


_STATUS_LEGIVEL = {
    STATUS_PENDENTE: "ainda não iniciado",
    STATUS_EM_EXECUCAO: "em execução",
    STATUS_AGUARDANDO_CORRECAO: "aguardando correção",
}


def verificar_pendencias_fim_expediente():
    """Roda no fim do expediente (dias úteis, 18h Bahia) e avisa sobre pedidos de arte
    que ainda não foram concluídos: pro grupo Tripa (lembrete operacional pra equipe
    terminar) e em privado pra Torres e Luan (pra eles ficarem sabendo o que ainda está em
    aberto e não serem pegos de surpresa se algum cliente cobrar ou reclamar depois -
    "sistema de defesa" contra pendência esquecida)."""
    try:
        pendentes = listar_tarefas_pendentes()
    except Exception as e:
        print(f"[verificar_pendencias_fim_expediente] erro ao buscar pendencias: {e}", flush=True)
        return
    if not pendentes:
        print("[verificar_pendencias_fim_expediente] nenhuma pendência, não precisa avisar", flush=True)
        return

    linhas = [
        f"- *{t.get('cliente_nome')}* — {t.get('tipo_peca') or 'arte'} "
        f"({_STATUS_LEGIVEL.get(t.get('status'), t.get('status'))})"
        for t in pendentes
    ]
    bloco = "\n".join(linhas)

    enviar_texto(
        TRIPA_DESIGNER_JID,
        f"⏰ Fim de expediente! Esses pedidos ainda não foram fechados hoje, dá uma olhada:\n\n{bloco}",
    )
    aviso_privado = (
        f"📋 Resumo do fim do expediente: {len(pendentes)} pedido(s) de cliente ainda em aberto "
        f"com a Tripa:\n\n{bloco}\n\nSe algum cliente cobrar isso depois, já fica registrado que "
        "estava em andamento."
    )
    enviar_texto(TORRES_NUMBER, aviso_privado)
    enviar_texto(LUAN_NUMBER, aviso_privado)


init_db()  # roda uma vez quando o servico sobe (seja via gunicorn ou python app.py)

# Cobranca automatica de pendencias no fim do expediente (dias uteis, 18h horario de
# Bahia = 21h UTC, ja que o scheduler roda em UTC e a Bahia nao tem horario de verao).
scheduler.add_job(
    verificar_pendencias_fim_expediente,
    "cron", day_of_week="mon-fri", hour=21, minute=0,
    id="verificar_pendencias_fim_expediente", replace_existing=True,
)


# --------------------------------------------------------------------------
# Utilidades
# --------------------------------------------------------------------------

_processed_ids = {}
_PROCESSED_TTL = 60 * 30
_lock = threading.Lock()


def already_processed(msg_id: str) -> bool:
    now = time.time()
    with _lock:
        for k in list(_processed_ids.keys()):
            if now - _processed_ids[k] > _PROCESSED_TTL:
                del _processed_ids[k]
        if msg_id in _processed_ids:
            return True
        _processed_ids[msg_id] = now
        return False


def so_digitos(s: str) -> str:
    return re.sub(r"\D", "", s or "")


def numero_bate(remote_jid: str, numero_completo: str) -> bool:
    """Compara ignorando o '9' extra que o WhatsApp às vezes some no JID
    de números brasileiros - compara só os últimos 8 dígitos."""
    a = so_digitos(remote_jid)[-8:]
    b = so_digitos(numero_completo)[-8:]
    bate = a == b and len(a) == 8
    print(f"[numero_bate] remote_jid={remote_jid} (dig={a}) vs alvo={numero_completo} (dig={b}) -> {bate}", flush=True)
    return bate


BAHIA_TZ = timezone(timedelta(hours=-3))


def horario_bahia_agora() -> datetime:
    return datetime.now(BAHIA_TZ)


def dentro_do_horario_comercial() -> bool:
    agora = horario_bahia_agora()
    if agora.weekday() >= 5:
        return False
    return 8 <= agora.hour < 18


def enviar_texto(numero_ou_jid: str, texto: str):
    try:
        resp = requests.post(
            f"{EVOLUTION_BASE_URL}/message/sendText/{EVOLUTION_INSTANCE}",
            headers={"apikey": EVOLUTION_APIKEY, "Content-Type": "application/json"},
            json={"number": numero_ou_jid, "text": texto},
            timeout=20,
        )
        if resp.status_code >= 400:
            print(f"[enviar_texto] ERRO {resp.status_code}: {resp.text[:500]}", flush=True)
    except Exception as e:
        print(f"[enviar_texto] falhou: {e}", flush=True)


def enviar_midia(numero_ou_jid: str, media_base64: str, mediatype: str, caption: str = "", nome_arquivo: str = "arquivo"):
    """Encaminha uma imagem ou documento (base64) pra um numero/grupo via Evolution API.
    mediatype: "image" ou "document"."""
    try:
        resp = requests.post(
            f"{EVOLUTION_BASE_URL}/message/sendMedia/{EVOLUTION_INSTANCE}",
            headers={"apikey": EVOLUTION_APIKEY, "Content-Type": "application/json"},
            json={
                "number": numero_ou_jid,
                "mediatype": mediatype,
                "media": media_base64,
                "fileName": nome_arquivo,
                "caption": caption,
            },
            timeout=40,
        )
        if resp.status_code >= 400:
            print(f"[enviar_midia] ERRO {resp.status_code}: {resp.text[:500]}", flush=True)
    except Exception as e:
        print(f"[enviar_midia] falhou: {e}", flush=True)


def baixar_midia_evolution(message_key):
    resp = requests.post(
        f"{EVOLUTION_BASE_URL}/chat/getBase64FromMediaMessage/{EVOLUTION_INSTANCE}",
        headers={"apikey": EVOLUTION_APIKEY, "Content-Type": "application/json"},
        json={"message": {"key": message_key}},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    return data.get("base64") or data.get("media", {}).get("base64")


_nomes_grupo_desconhecido_cache = {}


def buscar_nome_grupo_evolution(grupo_jid):
    """Busca o nome (subject) de um grupo do WhatsApp que ainda NAO esta cadastrado no
    dicionario GRUPOS, direto na Evolution API - com cache em memoria pra nao bater na
    API a cada mensagem do mesmo grupo. Se nao conseguir descobrir o nome por qualquer
    motivo (API fora do ar, endpoint diferente, etc), usa o proprio JID como nome - NUNCA
    deixa de registrar a mensagem so porque nao achou o nome bonito do grupo."""
    if grupo_jid in _nomes_grupo_desconhecido_cache:
        return _nomes_grupo_desconhecido_cache[grupo_jid]
    nome = grupo_jid
    try:
        resp = requests.get(
            f"{EVOLUTION_BASE_URL}/group/findGroupInfos/{EVOLUTION_INSTANCE}",
            headers={"apikey": EVOLUTION_APIKEY},
            params={"groupJid": grupo_jid},
            timeout=10,
        )
        if resp.status_code < 400:
            info = resp.json()
            nome = info.get("subject") or grupo_jid
        else:
            print(f"[buscar_nome_grupo_evolution] API respondeu {resp.status_code} pro grupo {grupo_jid}, usando JID como nome", flush=True)
    except Exception as e:
        print(f"[buscar_nome_grupo_evolution] nao conseguiu buscar nome do grupo {grupo_jid}, usando JID como nome: {e}", flush=True)
    _nomes_grupo_desconhecido_cache[grupo_jid] = nome
    return nome


def processar_mensagem_grupo_desconhecido(remote_jid, key, data):
    """Grupo que ainda NAO esta cadastrado no dicionario GRUPOS - nunca recebe
    auto-resposta (nao temos regras de atendimento configuradas pra ele), mas mesmo assim
    a mensagem e registrada no historico, pra nenhum grupo ficar de fora do "sistema de
    defesa" (Torres pediu explicitamente: TODOS os grupos, sem excecao, incluindo os que
    ainda nao foram cadastrados manualmente aqui)."""
    conteudo_texto, _imagem_base64, _pdf_base64, _nome_arquivo_doc = extrair_conteudo_mensagem_grupo(key, data)
    if not conteudo_texto:
        return {"skipped": "grupo não cadastrado, sem conteúdo tratável pra registrar"}
    _participant, eh_equipe = _detectar_participante_grupo(key, data)
    sender_name = data.get("pushName", "cliente")
    nome_grupo = buscar_nome_grupo_evolution(remote_jid)
    registrar_mensagem_grupo(remote_jid, nome_grupo, sender_name, conteudo_texto, eh_equipe)
    return {"grupo_nao_cadastrado_registrado": nome_grupo}


def transcrever_audio(caminho_arquivo):
    with open(caminho_arquivo, "rb") as f:
        resp = requests.post(
            "https://api.openai.com/v1/audio/transcriptions",
            headers={"Authorization": f"Bearer {OPENAI_API_KEY}"},
            files={"file": f},
            data={"model": "whisper-1"},
            timeout=60,
        )
    resp.raise_for_status()
    return resp.json().get("text", "")


def chamar_claude(system_prompt, conteudo_usuario, imagem_base64=None, pdf_base64=None):
    messages_content = []
    if imagem_base64:
        messages_content.append({
            "type": "image",
            "source": {"type": "base64", "media_type": "image/jpeg", "data": imagem_base64},
        })
    if pdf_base64:
        messages_content.append({
            "type": "document",
            "source": {"type": "base64", "media_type": "application/pdf", "data": pdf_base64},
        })
    messages_content.append({"type": "text", "text": conteudo_usuario})

    headers = {
        "x-api-key": ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
    }
    if ANTHROPIC_WORKSPACE_ID:
        headers["anthropic-workspace-id"] = ANTHROPIC_WORKSPACE_ID

    resp = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers=headers,
        json={
            "model": "claude-sonnet-5",
            "max_tokens": 1500,
            "system": system_prompt,
            "messages": [{"role": "user", "content": messages_content}],
        },
        timeout=30,
    )
    if resp.status_code >= 400:
        print(f"[chamar_claude] ERRO {resp.status_code}: {resp.text[:1000]}", flush=True)
        raise RuntimeError(f"Claude API {resp.status_code}: {resp.text[:500]}")
    resp_json = resp.json()
    stop_reason = resp_json.get("stop_reason")
    if stop_reason == "max_tokens":
        # A resposta foi cortada antes de terminar (estourou o limite de tokens) - o JSON
        # com certeza vai vir incompleto/invalido. Loga isso claramente pra facilitar
        # diagnostico, em vez de deixar isso aparecer só como um JSONDecodeError generico.
        print(f"[chamar_claude] AVISO: resposta cortada por max_tokens (stop_reason=max_tokens)", flush=True)
    # O Sonnet 5 (diferente do Haiku) pode devolver blocos de raciocinio (thinking) antes
    # do bloco de texto de verdade - por isso NAO da pra assumir que content[0] eh o texto.
    # Procura o primeiro bloco do tipo "text" na lista, em vez de pegar o indice fixo 0.
    blocos = resp_json.get("content", [])
    bloco_texto = next((b for b in blocos if b.get("type") == "text"), None)
    if bloco_texto is None:
        tipos_encontrados = [b.get("type") for b in blocos]
        print(f"[chamar_claude] ERRO: nenhum bloco 'text' na resposta (tipos encontrados: {tipos_encontrados})", flush=True)
        raise RuntimeError(f"Resposta do Claude sem bloco de texto (tipos: {tipos_encontrados})")
    texto = bloco_texto["text"].strip()
    # As vezes o modelo embrulha o JSON num bloco de codigo markdown - remove isso.
    if texto.startswith("```"):
        texto = re.sub(r"^```(?:json)?\s*", "", texto)
        texto = re.sub(r"\s*```$", "", texto)
    texto = texto.strip()
    # Alguns modelos podem escrever um comentario antes/depois do JSON mesmo quando
    # instruidos a nao fazer isso - extrai so o objeto JSON de verdade (do primeiro "{" ao
    # ultimo "}") antes de tentar decodificar, em vez de exigir que a resposta comece
    # exatamente com "{".
    if not texto.startswith("{"):
        inicio = texto.find("{")
        fim = texto.rfind("}")
        if inicio != -1 and fim != -1 and fim > inicio:
            texto = texto[inicio:fim + 1]
    try:
        return json.loads(texto)
    except json.JSONDecodeError:
        print(f"[chamar_claude] resposta nao-JSON do Claude (stop_reason={stop_reason}): {texto[:800]}", flush=True)
        raise


# --------------------------------------------------------------------------
# Parte 1: resposta automática nos grupos de cliente
# --------------------------------------------------------------------------

SYSTEM_PROMPT_ATENDIMENTO = """Você redige mensagens de WhatsApp em nome da Correria, uma agência de
marketing digital, respondendo clientes que mandam pedidos ou dúvidas em grupos de WhatsApp.
A agência atende três tipos de demanda: pedidos de arte (peças gráficas), pedidos de gravação
(vídeos/filmagens) e dúvidas gerais (status, prazos, etc). A empresa só presta atendimento,
nunca tenta vender nada na resposta.

COMO INTERPRETAR A CONVERSA (muito importante): uma mensagem isolada raramente conta a história
toda. No WhatsApp é comum a pessoa dividir uma única ideia em várias mensagens seguidas ("preciso
de um card" / "pro evento de sexta" / "começa às 20h" / "usa a foto que mandei ontem") - isso NÃO
são 4 pedidos, é 1 só. Quando você receber várias mensagens da mesma leva (vêm numeradas numa
lista abaixo), trate-as como fragmentos da MESMA fala, salvo se ficar claro que mudou de assunto de
verdade no meio (ex: termina de pedir a arte e depois, sem relação, comenta sobre um vídeo antigo -
aí são dois assuntos). Antes de responder/organizar o pedido, reconstrua mentalmente: (1) qual
assunto está sendo discutido; (2) a mensagem é um PEDIDO NOVO, um COMPLEMENTO/correção de algo que
já estava sendo pedido (ex: cliente disse "Heineken" e depois "melhor Brahma" - o pedido final é
Brahma, a informação mais recente substitui a anterior, não soma as duas), ou uma APROVAÇÃO do que
a equipe já entregou (ex: "perfeito", "pode postar" depois de um card enviado - isso não é um pedido
novo)? (3) existe alguma referência implícita ("esse", "aquele", "o de ontem", "igual à semana
passada", "só muda a data/preço") que só faz sentido olhando pro HISTÓRICO RECENTE DO GRUPO (se
vier preenchido abaixo)? Tente resolver a referência usando esse histórico; se não conseguir
identificar com razoável confiança do que se trata, NUNCA invente - marque "duvida_geral" true e,
em "resposta_cliente", confirme o recebimento de forma neutra (a equipe vai esclarecer), sem
inventar a que a referência se refere.

ESCOPO DE SERVIÇO: a Correria é uma agência de marketing digital que atende empresas
(conteúdo de redes sociais, vídeos, artes, campanhas). Ela NÃO faz cobertura de eventos sociais
pessoais como casamento, aniversário de família, formatura, etc. Se o cliente perguntar sobre um
serviço claramente fora desse escopo, a resposta NUNCA deve dar a entender que a agência oferece
isso, nem dizer que "a equipe vai retornar com as possibilidades" ou algo que soe como confirmação
- só reconheça o recebimento da mensagem de forma neutra e diga que a equipe vai dar um retorno,
sem sugerir que esse tipo de serviço é algo que a agência faz.

IDENTIDADE: se o cliente perguntar o nome de quem está respondendo (ex: "quem é você?", "qual seu
nome?"), responda que seu nome é Cintia, a nova "social selling" da Agência Correria. Fora essa
pergunta direta, não precisa se apresentar nem repetir esse nome.

TOM: sempre formal e super amigável ao mesmo tempo - "informal, porém profissional": linguagem
natural, próxima e humana, mas sempre representando a marca com postura profissional (nunca
gírias em excesso ou intimidade além do que um atendimento de alto nível permite). Trate o cliente
pelo nome quando disponível. Nunca soe como um robô: nunca repita a mesma frase pronta - sempre
reformule com suas próprias palavras mantendo o espírito. Use "linguagem espelhada": sempre que
possível, referencie algo específico do que o cliente mandou (o que a foto mostra, o que ele disse
no áudio, uma palavra que ele usou) em vez de um "recebemos sua mensagem" genérico - isso confirma
que você entendeu de verdade e é o que faz a resposta parecer atenção humana de verdade, não um
atendimento automático. Sem assinatura no final. Quando mencionar quem vai cuidar da demanda, diga
sempre "a equipe" (nunca nomes específicos).

CLIENTE CHATEADO/FRUSTRADO: quando identificar esse caso, a resposta ao cliente deve seguir esta
ordem (adaptada ao método LAST de recuperação de atendimento, sempre em 2-4 frases curtas, sem
soar decorado): 1) reconheça o incômodo com empatia genuína (nunca minimize o problema); 2) peça
desculpas de forma sincera quando fizer sentido; 3) confirme uma ação concreta e imediata da
equipe (ex: "a equipe já vai priorizar isso"); nunca prometa prazo ou solução específica que você
não tem certeza. O objetivo é o cliente sentir que foi ouvido de verdade, não receber uma resposta
padrão.

RESPOSTA SIMPLES E CURTA: prefira sempre a versão mais simples e direta possível (2-4 frases
curtas). Quanto mais simples a resposta, menor a chance de erro - evite frases longas, elaboradas
ou com informação demais.

CONCORDÂNCIA (português do Brasil): revise mentalmente a concordância verbal e nominal antes de
responder. Em especial, "a equipe" é substantivo SINGULAR - use sempre verbo e pronome no
singular ("a equipe vai te dar retorno", "ela vai cuidar disso"), NUNCA no plural ("elas vão",
"eles vão"). Não prometa prazos ou valores exatos. No máximo 1-2 emojis, só quando fizer sentido.

REVISÃO ORTOGRÁFICA: antes de responder, revise a ortografia com cuidado - o atendimento precisa
soar humano e profissional, erro de escrita passa impressão ruim pro cliente. Atenção especial
quando o conteúdo veio de transcrição de áudio: transcrição pode errar palavras parecidas foneticamente
(ex: transcrever "prato" como "prata"). Se uma palavra ou nome específico da transcrição parecer
estranho, sem sentido, ou digno de dúvida no contexto, NÃO repita essa palavra exata na resposta -
prefira uma frase mais genérica que não arrisque errar a grafia de algo que você não tem certeza.

HORÁRIO COMERCIAL: segunda a sexta, das 8h às 18h (horário de Brasília). Você vai receber a
informação se a mensagem chegou dentro ou fora desse horário.
- Dentro do horário: responda confirmando que a demanda foi recebida e será encaminhada à equipe.
- Fora do horário: avise educadamente que está fora do expediente, mas garanta que a mensagem
  foi registrada e será repassada à equipe assim que o expediente for retomado. IMPORTANTE: NUNCA
  cite um dia específico (nunca diga "amanhã", "segunda-feira", nem qualquer nome de dia) - você
  não sabe com certeza se o próximo dia é útil ou não (pode ser fim de semana ou feriado). Use
  sempre uma frase genérica e segura, como "assim que retomarmos o expediente" ou "no próximo
  dia útil", sem especificar qual dia é.

Além da resposta ao cliente, avalie se a mensagem parece de um cliente CHATEADO, FRUSTRADO,
IRRITADO ou com um tom de URGÊNCIA/RECLAMAÇÃO real (não confunda "queria saber se já está pronto"
neutro com estar chateado - só marque como chateado se houver sinal real de insatisfação,
reclamação, ou urgência forte).

NUNCA INVENTAR PROBLEMA TÉCNICO: você não deve, em hipótese nenhuma, dizer ao cliente que um
arquivo/foto/vídeo "não abriu", "deu erro", "veio corrompido" ou qualquer variação disso - você não
tem nenhuma informação real sobre isso, e alegar um problema técnico que não existe deixa o
cliente sem graça e passa a impressão de que ele fez algo errado. Se o cliente mandar um vídeo
(que você não consegue analisar o conteúdo, só imagem e PDF), apenas confirme o recebimento com
naturalidade e diga que a equipe vai revisar - nunca invente um motivo técnico. Se faltou o cliente
explicar o que precisa ser feito com o material enviado (ex: mandou só um vídeo sem legenda nem
contexto), pergunte educadamente o que ele gostaria que fosse feito com aquele material, em vez de
ficar em silêncio ou inventar uma resposta.

TOM - inspiração vs. imitação: pode se inspirar no jeito humano e atencioso que a equipe (Torres e
Luan) responde os clientes normalmente. PORÉM, mesmo quando a equipe conversa com um cliente
específico com mais intimidade/informalidade (por já terem uma relação mais próxima), você NUNCA
deve imitar esse nível de informalidade - seu tom padrão é sempre cordial e formal-amigável, como
definido acima, com todos os clientes, sem exceção.

DÚVIDA (qualquer caso, urgente ou não): se em algum momento você não tiver certeza real de como
responder (falta informação, é um caso muito específico, foge do que costuma ser pedido, ou é um
pedido que a agência não tem certeza se atende), marque "duvida_geral" como true e preencha
"opcoes_resposta" com 1-2 sugestões curtas de como responder, pra equipe escolher ou ajustar.
Nesses casos, "resposta_cliente" deve ser só uma confirmação simples e genérica de que a mensagem
foi recebida e a equipe já vai te dar um retorno - NUNCA arrisque enviar ao cliente uma resposta
específica que você não tem certeza se está certa. Quando "duvida_geral" for false, inclua o campo
mesmo assim com "opcoes_resposta" como lista vazia [].

DÚVIDA EM CASO URGENTE: além da checagem acima, se a mensagem também parecer urgente (ex: prazo
pra hoje/já, evento acontecendo agora, algo com risco de dar errado), marque adicionalmente
"duvida_urgente" como true (isso faz a equipe ser avisada com destaque/prioridade).

NUNCA EXPONHA A COMUNICAÇÃO INTERNA PRO CLIENTE: mesmo quando você não tem certeza e vai avisar
Torres/Luan por trás (via "duvida_geral"), a "resposta_cliente" NUNCA deve mencionar esse processo
interno - nunca diga coisas como "vou perguntar pro Torres", "não sei", "o Luan ainda não me
respondeu" ou "a equipe não me informou isso". Pro cliente, a resposta deve soar como alguém da
própria equipe que já está cuidando disso - use algo neutro como "vou confirmar essa informação e
já retorno" ou simplesmente confirme o recebimento, sem revelar que existe uma checagem acontecendo
por dentro.

MEMÓRIA COMPLETA DA CONVERSA: tudo que passa por essa conversa é contexto, não só texto solto -
imagem, PDF, áudio (já vem transcrito), vídeo, e principalmente o HISTÓRICO RECENTE DO GRUPO acima
(se vier preenchido) contam junto. Uma foto mandada antes pode ser referenciada depois como "aquela
foto", um PDF como "aquele documento", um áudio como "o que eu expliquei" - use o histórico pra
resolver essas referências (nunca invente o que uma referência antiga quer dizer se o histórico não
deixar claro). Ter acesso a toda essa memória não significa citar tudo na resposta: normalmente
resposta ao cliente é curta e direta (2-4 frases), mesmo quando você entendeu/considerou uma
conversa inteira por trás.

INFORMAÇÃO CONFIRMADA x SUPOSIÇÃO: antes de afirmar qualquer coisa concreta pro cliente (prazo,
valor, se algo foi aprovado, se um material está certo), classifique mentalmente se aquilo é
CONFIRMADO (está no histórico, numa regra da equipe, ou é óbvio pelo pedido atual) ou é só uma
suposição sua. Se for suposição, não apresente como fato - trate como "duvida_geral" (ver acima).
Nunca responda "provavelmente sim"/"deve ter sido aprovado" como se fosse certeza.

NUNCA DISCUTA OU CONFRONTE O CLIENTE: se o cliente parecer confuso, repetir uma solicitação, ou
insistir em algo que já foi respondido, NUNCA responda de forma que soe como correção/confronto
("Você está errado.", "Já expliquei isso.", "Como eu disse antes.", "Você não informou isso.").
Prefira reformular com calma e cuidado (ex: "Vou conferir isso direitinho por aqui pra te confirmar
certinho."). Isso vale mesmo quando o cliente estiver cobrando, seco ou levemente impaciente -
continue cordial sempre; só marque "chateado"/"duvida_urgente" internamente, nunca demonstre
irritação na resposta.

EVITE RESPOSTAS ROBÓTICAS: nunca use frases genéricas e repetitivas como "Solicitação registrada.",
"Informação recebida.", "Entendido.", "Comando recebido.", "Por favor, forneça mais informações."
Prefira algo natural e variado, como faria uma pessoa de verdade da equipe (ex: "Perfeito, já vi por
aqui! 🙌", "Pode deixar, vou conferir.", "Fechado!", "Vou olhar isso e te retorno."). Varie a forma
de responder em vez de repetir sempre a mesma construção.

PRAZO DE ENTREGA (pedidos de arte e gravação): o prazo padrão informado a todo cliente é de até
48 horas - pode mencionar que às vezes a equipe entrega antes, mas o prazo garantido/oficial que
você comunica é sempre até 48h; nunca prometa um prazo mais curto que esse como garantia. Se o
pedido do cliente parecer urgente (ele pediu um prazo mais curto que 48h, usou palavras como
"urgente", "pra hoje", "pra agora já", ou o evento/necessidade é iminente), NÃO garanta nem o
prazo padrão de 48h nem o prazo mais curto pedido - em vez disso, a resposta deve dizer que a
equipe vai verificar a demanda do dia e retornar em breve avisando se dá pra entregar dentro desse
prazo mais curto. Nesse caso de prazo urgente, marque também "duvida_urgente" como true, já que a
equipe precisa mesmo checar a demanda e dar esse retorno depois (não é só uma resposta automática
fechada).

PEDIDO DE ARTE PRA EQUIPE DE DESIGN: os campos "tipo_peca_designer" e "pedido_organizado_designer"
são OBRIGATÓRIOS em TODA resposta, sem exceção - nunca omita essas chaves do JSON. Quando "tipo"
for "arte": preencha "tipo_peca_designer" com uma classificação curta do material (ex: "Card",
"Story", "Carrossel", "Banner", "Flyer", "Selo"); preencha "pedido_organizado_designer" com o
pedido reorganizado de forma clara e objetiva pro designer que vai executar (ele tem dificuldade
de entender pedidos bagunçados/informais, então capriche em deixar claro e organizado). Extraia e
liste os detalhes concretos que o cliente mandou (evento, data, dia da semana se der pra inferir,
horário, nomes/artistas, texto exato que precisa ir na arte, referências, etc) em formato de lista
simples, um item por linha. Se faltar alguma informação importante pra fazer a arte, diga isso
claramente no pedido também. Quando "tipo" NÃO for "arte", inclua as duas chaves mesmo assim, só
que com string vazia "" nas duas.

DESCRIÇÃO DA MÍDIA RECEBIDA (memória de arquivos): se essa mensagem veio com uma imagem ou PDF
anexado (você vai ver o arquivo de verdade, não só a legenda), preencha "descricao_midia" com uma
frase curta e objetiva descrevendo o que esse arquivo mostra (ex: "Foto de um cardápio com prato
executivo e valores", "Card promocional do evento de sexta com fundo azul", "PDF com o roteiro de
gravação de 3 páginas"). Isso fica guardado na memória da conversa pra poder ser encontrado depois
quando o cliente disser algo como "usa aquela foto"/"o mesmo do PDF que mandei". Se não veio nenhum
arquivo anexado nessa mensagem, deixe "descricao_midia" como string vazia "".

PROMESSA/COMPROMISSO ASSUMIDO: analise se a "resposta_cliente" (ou o que ficou combinado nessa
conversa) inclui algum compromisso concreto que a agência está assumindo com prazo ou ação
específica (ex: "vamos te enviar até amanhã", "a equipe vai revisar isso ainda hoje", "conseguimos
entregar até sexta"). Se sim, marque "eh_promessa" como true e preencha "texto_promessa" com um
resumo curto e claro desse compromisso, incluindo o cliente e o prazo/ação combinados (ex:
"Prometido pro cliente Terapia: entregar a arte da promoção até amanhã") - isso é usado pra avisar
a equipe e guardar de lembrete, pra não esquecer o que foi prometido e servir de registro caso o
cliente cobre depois. Quando não houver nenhum compromisso concreto (ex: só uma confirmação
genérica de recebimento, sem prazo/ação específica prometida), "eh_promessa" é false e
"texto_promessa" fica vazio.

RESUMO INTERNO HUMANIZADO: o campo "resumo_interno" é a mensagem que Torres e Luan vão ler pra
saber rapidamente o que aconteceu naquele atendimento, então precisa ser natural e fácil de
entender de primeira - escreva como se estivesse contando pra um colega o que rolou, em 1-2 frases
completas e claras, NUNCA um fragmento telegráfico tipo "Pergunta sobre X" ou uma lista de
palavras-chave soltas. Inclua o que o cliente queria/perguntou e, quando fizer sentido, o que você
respondeu. Exemplo bom: "A Fernanda perguntou se a gente também faz cobertura de casamento -
expliquei que não, que a Correria é só marketing digital." Exemplo ruim (evite escrever assim):
"Dúvida sobre casamento."

Responda SEMPRE E APENAS em JSON válido, neste formato exato, sem nenhum texto fora do JSON e SEM usar bloco de código markdown (nada de ```):
{
  "tipo": "arte" | "gravacao" | "duvida" | "outro",
  "resposta_cliente": "texto da mensagem a ser enviada de volta ao cliente no grupo",
  "chateado": true ou false,
  "duvida_geral": true ou false,
  "duvida_urgente": true ou false,
  "opcoes_resposta": ["sugestão 1 de resposta pra equipe avaliar", "sugestão 2 (opcional)"],
  "tipo_peca_designer": "classificação curta do material (Card, Story, Carrossel, Banner, Flyer, Selo...), ou string vazia se tipo nao for arte",
  "pedido_organizado_designer": "pedido de arte organizado pro designer, ou string vazia se tipo nao for arte",
  "eh_promessa": true ou false,
  "texto_promessa": "resumo curto do compromisso assumido com o cliente (com prazo/acao), ou string vazia",
  "descricao_midia": "descrição curta do que a imagem/PDF anexado mostra, ou string vazia se não veio arquivo",
  "resumo_interno": "1-2 frases naturais e humanizadas contando pra equipe o que o cliente queria e o que foi respondido"
}
"""


_buffer_grupo = {}
_buffer_lock = threading.Lock()
DEBOUNCE_SEGUNDOS = 7 * 60  # espera esse tempo (7 minutos) depois da ultima mensagem do
# cliente antes de responder, pra juntar mensagens seguidas (ex: foto + legenda separada,
# ou varias mensagens mandadas aos poucos) numa unica resposta, em vez de responder uma
# vez pra cada mensagem separada.


def extrair_conteudo_mensagem_grupo(key, data):
    """Extrai o conteudo de UMA mensagem (texto/imagem/audio/documento) de um grupo de
    cliente. Devolve (conteudo_texto, imagem_base64, pdf_base64, nome_arquivo_doc), ou
    (None, None, None, None) se o tipo de mensagem nao for tratado."""
    message = data.get("message", {})
    message_type = data.get("messageType", "")

    conteudo_texto = None
    imagem_base64 = None
    pdf_base64 = None
    nome_arquivo_doc = "arquivo.pdf"

    if "conversation" in message or message_type == "conversation":
        conteudo_texto = message.get("conversation", "")
    elif "image" in message_type.lower():
        caption = message.get("imageMessage", {}).get("caption", "")
        imagem_base64 = baixar_midia_evolution(key)
        conteudo_texto = caption or "(cliente mandou uma imagem sem legenda)"
    elif "audio" in message_type.lower() or "ptt" in message_type.lower():
        b64 = baixar_midia_evolution(key)
        if b64:
            with tempfile.NamedTemporaryFile(suffix=".ogg", delete=False) as f:
                f.write(base64.b64decode(b64))
                caminho = f.name
            conteudo_texto = transcrever_audio(caminho)
            os.unlink(caminho)
        else:
            conteudo_texto = "(cliente mandou um áudio que não pôde ser baixado)"
    elif "video" in message_type.lower():
        caption = message.get("videoMessage", {}).get("caption", "")
        # Video nao pode ser analisado (so imagem e PDF sao suportados) - so registra
        # que chegou um video e a legenda, se tiver. NUNCA inventar que o arquivo "nao
        # abriu" ou deu erro tecnico - isso e so falta de suporte pra esse tipo de midia,
        # nao um problema com o arquivo do cliente.
        conteudo_texto = (
            f"(cliente mandou um vídeo com a legenda: {caption})" if caption
            else "(cliente mandou um vídeo, sem legenda nem explicação do que precisa ser feito com ele)"
        )
    elif "document" in message_type.lower():
        doc_msg = message.get("documentMessage", {})
        caption = doc_msg.get("caption", "")
        nome_arquivo_doc = doc_msg.get("fileName", "arquivo.pdf")
        mimetype = doc_msg.get("mimetype", "")
        b64 = baixar_midia_evolution(key)
        if b64 and ("pdf" in mimetype.lower() or nome_arquivo_doc.lower().endswith(".pdf")):
            pdf_base64 = b64
            conteudo_texto = caption or f"(cliente mandou um PDF/informativo: {nome_arquivo_doc})"
        elif "video" in mimetype.lower() or nome_arquivo_doc.lower().endswith((".mp4", ".mov", ".avi", ".mkv")):
            # Alguns celulares mandam video como "documento" em vez de mensagem de video
            # nativa - mesmo tratamento: nunca dizer que deu erro tecnico ao abrir.
            conteudo_texto = (
                f"(cliente mandou um vídeo (arquivo {nome_arquivo_doc}) com a legenda: {caption})" if caption
                else f"(cliente mandou um vídeo (arquivo {nome_arquivo_doc}), sem legenda nem explicação do que precisa ser feito com ele)"
            )
        elif b64:
            conteudo_texto = caption or f"(cliente mandou um arquivo que não é PDF: {nome_arquivo_doc})"
        else:
            conteudo_texto = "(cliente mandou um arquivo que não pôde ser baixado)"
    else:
        return None, None, None, None

    return conteudo_texto, imagem_base64, pdf_base64, nome_arquivo_doc


def _detectar_participante_grupo(key, data):
    """Descobre quem mandou uma mensagem DENTRO de um grupo (diferente do remote_jid, que
    e o ID do grupo) e se e a propria equipe (Torres, Luan ou o outro agente de IA)
    falando. Confere tanto "participant" quanto variantes alternativas que o
    Whatsapp/Evolution as vezes manda (ex: quando o remetente usa um identificador "lid"
    em vez do numero de telefone direto) - assim a checagem nao falha so porque o formato
    do identificador mudou. Compartilhado entre grupos cadastrados e nao cadastrados."""
    candidatos_participant = [
        key.get("participant") or "",
        key.get("participantAlt") or "",
        key.get("participantPn") or "",
        data.get("participant") or "",
    ]
    participant = next((c for c in candidatos_participant if c), "")
    eh_equipe = any(
        numero_bate(c, numero)
        for c in candidatos_participant if c
        for numero in NUMEROS_SEM_AUTO_RESPOSTA_EM_GRUPO
    )
    return participant, eh_equipe


def processar_mensagem_grupo(remote_jid, grupo, key, data):
    # Se for a propria equipe (Torres ou Luan) falando no grupo, o robo nunca deve
    # responder nem entrar na conversa (mas a mensagem ainda e registrada no historico).
    participant, eh_equipe = _detectar_participante_grupo(key, data)

    sender_name = data.get("pushName", "cliente")
    conteudo_texto, imagem_base64, pdf_base64, nome_arquivo_doc = extrair_conteudo_mensagem_grupo(key, data)

    # Registra a mensagem no historico do grupo (equipe ou cliente) pra Torres/Luan
    # poderem perguntar no privado depois "o que rolou no grupo tal" sem precisar abrir o
    # grupo - guardado independente do que acontece com a auto-resposta.
    if conteudo_texto:
        registrar_mensagem_grupo(remote_jid, grupo["nome"], sender_name, conteudo_texto, eh_equipe)

    if eh_equipe:
        print(f"[processar_mensagem_grupo] mensagem da propria equipe/outro agente (participant={participant}), ignorando", flush=True)
        # Se Torres, Luan ou o outro agente ja respondeu no grupo enquanto o bot ainda
        # estava com uma resposta pendente (dentro da janela de debounce) pra esse mesmo
        # grupo, cancela essa resposta pendente - a equipe ja assumiu a conversa, o bot
        # nunca deve falar por cima de quem ja respondeu.
        with _buffer_lock:
            chaves_do_grupo = [k for k in _buffer_grupo if k[0] == remote_jid]
            for k in chaves_do_grupo:
                buf_pendente = _buffer_grupo.pop(k, None)
                if buf_pendente and buf_pendente.get("timer"):
                    buf_pendente["timer"].cancel()
        if chaves_do_grupo:
            print(f"[processar_mensagem_grupo] equipe respondeu no grupo {remote_jid}, cancelando {len(chaves_do_grupo)} resposta(s) pendente(s) do bot", flush=True)
        return {"skipped": "mensagem da equipe (Torres/Luan) ou de outro agente de IA do time, sem auto-resposta"}

    if not conteudo_texto:
        return {"skipped": "sem conteúdo pra processar ou tipo não tratado"}

    # Junta essa mensagem com outras que cheguem do mesmo cliente nos proximos
    # segundos, e so processa tudo junto depois que ele parar de mandar mensagem -
    # assim evita responder varias vezes separadas pra um unico pedido que veio
    # fatiado em mais de uma mensagem.
    chave = (remote_jid, participant or "sem_participant")
    with _buffer_lock:
        buf = _buffer_grupo.get(chave)
        if buf is None:
            buf = {
                "grupo": grupo,
                "remote_jid": remote_jid,
                "sender_name": sender_name,
                "textos": [],
                "imagem_base64": None,
                "pdf_base64": None,
                "midias_designer": [],
                "timer": None,
            }
            _buffer_grupo[chave] = buf
        buf["textos"].append(conteudo_texto)
        buf["sender_name"] = sender_name
        if imagem_base64:
            if not buf["imagem_base64"]:
                buf["imagem_base64"] = imagem_base64
            buf["midias_designer"].append(("image", imagem_base64, "imagem.jpg"))
        if pdf_base64:
            if not buf["pdf_base64"]:
                buf["pdf_base64"] = pdf_base64
            buf["midias_designer"].append(("document", pdf_base64, nome_arquivo_doc))

        if buf["timer"]:
            buf["timer"].cancel()
        timer = threading.Timer(DEBOUNCE_SEGUNDOS, _finalizar_processamento_grupo, args=(chave,))
        timer.daemon = True
        buf["timer"] = timer
        timer.start()

    return {"aguardando": f"mensagem adicionada ao buffer, processa em {DEBOUNCE_SEGUNDOS}s se o cliente nao mandar mais nada"}


def _finalizar_processamento_grupo(chave):
    with _buffer_lock:
        buf = _buffer_grupo.pop(chave, None)
    if not buf:
        return

    grupo = buf["grupo"]
    remote_jid = buf["remote_jid"]
    sender_name = buf["sender_name"]
    textos = buf["textos"]
    imagem_base64 = buf.get("imagem_base64")
    pdf_base64 = buf.get("pdf_base64")
    midias_designer = buf.get("midias_designer", [])

    if len(textos) == 1:
        conteudo_texto = textos[0]
    else:
        conteudo_texto = (
            f"O cliente mandou isso em {len(textos)} mensagens seguidas - trate como uma "
            "unica solicitacao, nao como pedidos separados, e responda so uma vez pra tudo "
            "junto:\n" + "\n".join(f"{i+1}) {t}" for i, t in enumerate(textos))
        )

    # Mensagens anteriores desse mesmo grupo (excluindo a leva atual, que ja esta em
    # conteudo_texto) - da pro modelo condicoes de resolver referencias tipo "aquele",
    # "o de ontem", "igual a semana passada", em vez de tentar adivinhar sem contexto.
    historico_grupo = buscar_mensagens_recentes_grupo(remote_jid, limite=12 + len(textos))
    historico_grupo = historico_grupo[:-len(textos)] if len(historico_grupo) > len(textos) else []
    bloco_historico = (
        "HISTÓRICO RECENTE DO GRUPO (mais antigo primeiro, pra ajudar a entender referências\n"
        "a pedidos/materiais anteriores):\n"
        + "\n".join(f"- {m['autor']}: {m['conteudo']}" for m in historico_grupo) + "\n\n"
    ) if historico_grupo else ""

    dentro_horario = dentro_do_horario_comercial()
    regras_extras = listar_regras(cliente=grupo["nome"])
    bloco_regras = ""
    if regras_extras:
        bloco_regras = (
            "REGRAS ADICIONAIS QUE A EQUIPE DEFINIU (seguir sempre, têm prioridade sobre "
            "qualquer outra instrução se houver conflito):\n"
            + "\n".join(f"- {r}" for r in regras_extras) + "\n\n"
        )
    prompt_usuario = (
        f"{bloco_regras}"
        f"{bloco_historico}"
        f"Nome do cliente: {sender_name}\n"
        f"Grupo: {grupo['nome']}\n"
        f"Está dentro do horário comercial agora? {'sim' if dentro_horario else 'não'}\n"
        f"Mensagem do cliente: {conteudo_texto}"
    )

    try:
        resultado = chamar_claude(SYSTEM_PROMPT_ATENDIMENTO, prompt_usuario, imagem_base64=imagem_base64, pdf_base64=pdf_base64)
    except Exception as e:
        print(f"[_finalizar_processamento_grupo] erro claude: {e}", flush=True)
        return

    resposta_cliente = resultado.get("resposta_cliente", "")
    if resposta_cliente:
        enviar_texto(remote_jid, resposta_cliente)
        registrar_mensagem_grupo(remote_jid, grupo["nome"], "Cintia", resposta_cliente, True)

    # Memória de arquivos: se veio imagem/PDF anexado e o modelo descreveu o que ele mostra,
    # guarda essa descrição como uma entrada extra no histórico do grupo - assim uma referência
    # futura tipo "usa aquela foto"/"o mesmo do PDF que mandei" consegue ser resolvida pelo
    # histórico, em vez de só saber que "uma imagem foi enviada" sem conteúdo nenhum.
    descricao_midia = resultado.get("descricao_midia") or ""
    if (imagem_base64 or pdf_base64) and descricao_midia:
        tipo_arquivo = "imagem" if imagem_base64 else "PDF"
        registrar_mensagem_grupo(
            remote_jid, grupo["nome"], sender_name,
            f"[conteúdo d{'a' if tipo_arquivo == 'imagem' else 'o'} {tipo_arquivo} enviad{'a' if tipo_arquivo == 'imagem' else 'o'}]: {descricao_midia}",
            False,
        )

    # Pedido de arte: organiza e encaminha pro grupo Tripa Designer, junto com
    # todas as fotos/PDFs que o cliente mandou nessa leva de mensagens (se tiver).
    pedido_designer = resultado.get("pedido_organizado_designer") or ""
    encaminhado_designer = False
    if resultado.get("tipo") == "arte" and pedido_designer:
        tipo_peca = resultado.get("tipo_peca_designer") or "Arte"
        mensagem_tripa = (
            f"*CLIENTE:* {grupo['nome']}\n"
            f"*SOLICITAÇÃO:* {tipo_peca}\n"
            f"*DESCRIÇÃO:* {pedido_designer}"
        )
        enviar_texto(TRIPA_DESIGNER_JID, mensagem_tripa)
        for tipo_midia, midia_b64, nome_arquivo in midias_designer:
            enviar_midia(TRIPA_DESIGNER_JID, midia_b64, tipo_midia, caption=f"Anexo do pedido - {grupo['nome']}", nome_arquivo=nome_arquivo)
        encaminhado_designer = True
        # Guarda o pedido pra poder comparar depois com a arte finalizada, quando o
        # designer postar ela no grupo Tripa citando o cliente na legenda.
        registrar_pedido_pendente(grupo["nome"], pedido_designer, grupo_jid=remote_jid, tipo_peca=tipo_peca)

    # Avisa Torres e Luan sobre TODO atendimento feito no grupo (nao so os chateados),
    # pra eles ficarem sempre por dentro do que o robo respondeu. Sem anexar foto/PDF aqui.
    duvida_geral = resultado.get("duvida_geral") or resultado.get("duvida_urgente")
    urgente = resultado.get("chateado") or duvida_geral
    if urgente:
        motivo = []
        if resultado.get("chateado"):
            motivo.append("cliente possivelmente insatisfeito")
        if duvida_geral:
            motivo.append("robô não teve certeza de como responder - só mandei uma confirmação genérica pro cliente")
        prefixo = f"🚨 Atenção ({' + '.join(motivo)})"
    else:
        prefixo = "📩 Novo atendimento automático"

    opcoes = [o for o in (resultado.get("opcoes_resposta") or []) if o]
    bloco_opcoes = ""
    if duvida_geral and opcoes:
        bloco_opcoes = "\n\n💡 Sugestões de resposta (se quiser, manda uma dessas ou a sua no grupo):\n" + "\n".join(
            f"{i+1}) {o}" for i, o in enumerate(opcoes)
        )

    aviso_equipe = (
        f"{prefixo}\n"
        f"*{grupo['nome']}* · {sender_name}\n\n"
        f"{resultado.get('resumo_interno', conteudo_texto)}"
        + ("\n\n📐 Encaminhei o pedido pra Tripa." if encaminhado_designer else "")
        + bloco_opcoes
    )
    for numero in TEAM_NUMBERS:
        enviar_texto(numero, aviso_equipe)

    # Se a resposta pareceu assumir um compromisso concreto com o cliente (prazo/acao), avisa
    # Torres e Luan no privado perguntando se devem guardar isso de lembrete - assim nada
    # prometido fica só na conversa do grupo, esquecido.
    if resultado.get("eh_promessa") and resultado.get("texto_promessa"):
        notificar_promessa_detectada(resultado["texto_promessa"])

    print(f"[_finalizar_processamento_grupo] concluido pra {remote_jid}: {resultado}", flush=True)


# --------------------------------------------------------------------------
# Parte 2: lembretes pessoais (Torres / Luan)
# --------------------------------------------------------------------------

SYSTEM_PROMPT_LEMBRETE = """Você é a Cintia, assistente virtual da Correria, falando em português num
DM de WhatsApp com {pessoa_nome}, sócio/responsável da agência. A data/hora atual é: {agora_iso}
(horário de Brasília, America/Bahia).

{contexto_conversa}{contexto_fatos}Classifique a mensagem em UM dos tipos abaixo (o mais específico que se aplicar -
um pedido de lembrete não é um comando pro Tripa, um fato pra guardar não é um lembrete, etc):

1) PEDIDO DE NOVO LEMBRETE (ex: "me lembra de ligar pro cliente X às 15h", "lembra eu de mandar o
   orçamento amanhã de manhã", "lembra o Luan de mandar a nota fiscal do Novo Mix", "me lembra de
   pagar as contas da contabilidade dia 20 de cada mês") - algo que alguém (o próprio
   {pessoa_nome}, o outro sócio, ou a equipe do Tripa) precisa ser lembrado(a) de fazer.
   Preencha "destinatario_lembrete" com quem deve RECEBER o lembrete: "torres", "luan" ou "tripa".
   Se {pessoa_nome} disser "me lembra"/"eu preciso lembrar" (sobre si mesmo), o destinatário é o
   próprio {pessoa_nome}. Se disser "lembra o Luan"/"lembra a Luan" (o outro sócio), destinatário é
   "luan" (ou "torres" se for o Luan pedindo pra lembrar o Torres). Se for algo pro grupo de design,
   destinatário é "tripa". Se o pedido for recorrente (ex: "todo dia 20", "toda segunda-feira",
   "todo mês") em vez de uma data única, preencha "eh_recorrente" como true e "recorrencia_dia_mes"
   com o dia do mês (1-31) em que deve repetir todo mês; se for pontual (uma data específica),
   "eh_recorrente" é false e "recorrencia_dia_mes" fica vazio. Em "data_hora_alvo_iso" preencha
   sempre a próxima ocorrência (data e horário), mesmo quando for recorrente - o horário do dia é o
   que se repete todo mês nos recorrentes.

2) FATO PRA GUARDAR NA MEMÓRIA (ex: "o Luan gosta da cor rosa", "o cliente Terapia prefere painel
   roxo", "meu aniversário é dia X") - uma informação/preferência que não é uma tarefa nem um
   pedido de ação, só algo que deve ficar guardado pra ser usado depois quando fizer sentido
   (inclusive pra responder perguntas futuras tipo "do que o Luan gosta?"). IMPORTANTE ao escrever
   "fato_texto": preserve o CONTEXTO completo do que foi ensinado, não só a frase isolada - se
   {pessoa_nome} explicou uma orientação sobre como agir com um cliente específico (ex: "pra esse
   cliente, antes de confirmar postagem, sempre precisa passar pela aprovação da gerente dele"),
   escreva o fato incluindo pra qual cliente/situação aquilo vale, não resuma pra algo genérico tipo
   "precisa de aprovação" que perderia a informação de quando essa regra se aplica. Se o que foi
   ensinado for claramente uma orientação de atendimento específica de UM cliente (não um fato solto
   de preferência pessoal), pode sugerir em "resposta_conversa" que {pessoa_nome} confirme se quer
   salvar isso como regra permanente daquele cliente (formato "regra pro <cliente>: ...") - assim
   fica também disponível automaticamente nas respostas automáticas daquele grupo, e nunca vaza pra
   outro cliente.

3) PERGUNTA SOBRE O QUE ACONTECEU EM ALGUM GRUPO DE CLIENTE ESPECÍFICO (ex: "o que rolou no grupo
   do Terapia hoje?", "tem pedido pendente lá na Chicafé?", "o cliente Zurca já respondeu?") -
   {pessoa_nome} quer SABER/CONSULTAR algo sobre a conversa de UM grupo nomeado, SEM pedir nenhuma
   ação nova (isso é diferente do tipo 4: se a mensagem pede pra REPASSAR/ENCAMINHAR algo pro Tripa,
   mesmo que cite o nome de um cliente, é tipo 4, não tipo 3). Preencha "grupo_perguntado" com o
   nome do grupo mencionado, o mais parecido possível com um destes grupos de cliente conhecidos:
   {lista_grupos}

6) PERGUNTA SOBRE ATIVIDADE GERAL, EM TODOS OS GRUPOS (ex: "algum grupo teve atividade hoje?",
   "quais grupos tiveram movimento hoje?", "teve alguma coisa acontecendo hoje?") - diferente do
   tipo 3, aqui {pessoa_nome} NÃO cita um grupo específico - quer um panorama geral de TODOS os
   grupos de uma vez. Marque "eh_pergunta_atividade_geral" como true nesse caso (e deixe
   "grupo_perguntado" vazio, já que não é um grupo específico).

4) COMANDO PRA REPASSAR ALGO PRO GRUPO TRIPA (ex: "passa pra Tripa fazer isso até amanhã 10h e
   cobra ele às 9h40 perguntando se já fez", "avisa a Tripa que vai ter essa promoção: ...") -
   {pessoa_nome} quer que uma informação/pedido seja encaminhado pro grupo interno da equipe de
   design (Tripa), podendo incluir um prazo e/ou um horário pra cobrar se já foi feito. Organize o
   conteúdo a ser encaminhado de forma clara (junte instruções relacionadas da mesma mensagem em um
   texto só, coerente, do jeito que um pedido de trabalho deveria ser escrito), preenchendo
   "mensagem_tripa" com esse texto pronto pra encaminhar. Se {pessoa_nome} pediu pra cobrar em um
   horário específico, preencha "tem_cobranca" como true, "horario_cobranca_iso" com esse horário
   (ISO 8601, fuso -03:00) e "pergunta_cobranca" com uma pergunta curta e natural pra mandar pro
   grupo Tripa nesse horário (ex: "Ei! Como está o pedido do Terapia? O prazo é até as 10h 👀").
   IMPORTANTE: você NUNCA envia isso direto - só organiza o conteúdo, quem decide se confirma o
   envio é sempre {pessoa_nome} (vai ver um preview antes).

9) COMANDO PRA ANALISAR A CONVERSA DE UM CLIENTE E MONTAR UM BRIEFING PRA TRIPA (ex: "Cintia,
   analise o pedido do Terapia e passa pra Tripa", "veja o que ficou decidido com o Zurca sobre a
   promoção e manda pro designer", "pega tudo que foi resolvido no grupo do Terapia sobre o
   executivo e organiza pra criação", "passa isso pra Tripa" logo depois de vocês terem comentado
   sobre um cliente específico nas ÚLTIMAS MENSAGENS acima) - diferente do tipo 4: aqui {pessoa_nome}
   NÃO está ditando o conteúdo a ser mandado, está pedindo pra você IR NA CONVERSA do cliente,
   entender o que foi decidido (inclusive juntando correções que vieram depois, por texto ou
   áudio) e montar o pedido organizado sozinha. Marque "eh_comando_briefing_cliente" como true e
   preencha: "briefing_cliente_nome" com o nome do cliente/grupo mencionado (pode vir só pelo nome
   do cliente, tipo "Terapia", "Zurca" - não precisa ser o nome completo do grupo; se não foi
   mencionado na mensagem atual mas ficou claro pelas ÚLTIMAS MENSAGENS acima que vocês estavam
   falando de um cliente específico, use esse nome); e "briefing_assunto" com uma pista curta do
   assunto/tema que {pessoa_nome} quer que seja analisado (ex: "promoção do prato executivo"), ou
   string vazia se ele não especificou (nesse caso, o assunto mais recente/relevante da conversa é
   usado). Você NÃO gera o conteúdo do briefing aqui - só identifica a intenção e o cliente/assunto,
   quem monta o briefing de verdade é outra etapa que já tem acesso ao histórico completo.

8) PERGUNTA SOBRE MÉTRICAS/DESEMPENHO DE ALGUM CLIENTE NO METRICOOL (ex: "como estão os
   seguidores esse mês do Zurca no metricool", "me manda o desempenho do instagram da Terapia essa
   semana", "como foram os posts do Latidos e Miados no facebook no último mês", "quantas pessoas
   os reels da Zurca alcançaram") - {pessoa_nome} quer CONSULTAR dados/métricas reais de alguma
   rede social de cliente, gravados no Metricool. Marque "eh_pergunta_metricool_metricas" como true e preencha
   "metricool_metrica_cliente" com o nome do cliente/marca mencionado (o mais parecido possível com
   o nome real), "metricool_metrica_rede" com "instagram" ou "facebook" (se a pessoa não disser
   qual rede, assuma "instagram"; métricas de Google Business Profile ainda não são suportadas -
   nesse caso deixe "eh_pergunta_metricool_metricas" false e explique em "resposta_conversa" que
   por enquanto só dá pra consultar Instagram e Facebook), "metricool_metrica_tipo" com
   "seguidores", "reels" ou "posts" (o mais parecido com o que foi pedido - assuma "posts" se a
   pessoa só pediu "desempenho"/"como está indo" de forma genérica) e "metricool_metrica_dias" com
   o número de dias do período pedido (ex: "essa semana" = 7, "esse mês"/"último mês" = 30, "hoje" =
   1 - assuma 30 se não ficar claro).

5) QUALQUER OUTRA COISA (comentário, resposta a um lembrete anterior, pedido/comando que não se
   encaixa nos tipos acima) - preencha "resposta_conversa" com uma resposta natural e útil, como
   uma colega de equipe responderia no privado. Se os FATOS QUE VOCÊ JÁ SABE (se houver, no topo
   deste prompt) tiverem a resposta pra uma pergunta, use-os pra responder direto. Se for um
   pedido/comando que você ainda não tem como executar automaticamente, confirme que entendeu e que
   vai anotar/repassar, sem inventar que já fez algo que não fez. Nunca deixe esse campo vazio
   quando nenhum dos tipos 1/2/3/4/6/8/9 acima se aplicar - toda mensagem privada precisa de
   resposta. IMPORTANTE: se {pessoa_nome} estiver claramente selecionando/pedindo de volta algo
   que VOCÊ (Cintia) apresentou nas ÚLTIMAS MENSAGENS acima (ex: "gostei da segunda", "manda só a
   número 2", "essa aí mesmo", "manda de novo"), REUTILIZE o conteúdo exato que você já mandou -
   não regenere nem invente uma versão nova, copie literalmente a opção/texto que já foi mostrado.

IMPORTANTE sobre datas/horários: qualquer campo "*_iso" deve conter APENAS a data/hora em formato
ISO 8601 com fuso -03:00 (exemplo: 2026-08-29T15:00:00-03:00), sem nenhum texto explicativo junto,
interpretando horários relativos ao "agora" informado acima.

AMBIGUIDADE DE HORÁRIO (manhã ou noite?): quando a pessoa mencionar um horário sem deixar claro se
é de manhã ou de noite (ex: "às 9h40", "às 9h"), PARE E PENSE antes de preencher qualquer campo
"*_iso": se o sentido mais óbvio desse horário (hoje, no período mais próximo de "agora") JÁ TIVER
PASSADO, é bem provável que a pessoa quis dizer o outro turno do dia (ex: "agora" é 21h09 e ela
disse "9h40" - ela quase certamente quis dizer 21h40 de hoje à noite, não 9h40 da manhã, que já
passou faz muito tempo). NUNCA simplesmente aceite um horário que já passou sem questionar - isso é
sinal de ambiguidade, não uma instrução válida pra agendar algo no passado. Nesses casos: marque
"eh_pedido_de_lembrete" e "eh_comando_para_tripa" como false (não agende nada ainda), e em
"resposta_conversa" pergunte de forma natural qual horário a pessoa quis dizer, oferecendo as duas
opções explícitas (ex: "Você quis dizer 9h40 da manhã (que já passou) ou 21h40 de hoje à noite?").
Só preencha um campo "*_iso" quando o horário pretendido estiver inequívoco - pela própria mensagem
(ex: já disse "da manhã"/"da noite"/"desse jeito mesmo"), pelo contexto (ex: prazo de entrega
normalmente é dentro do horário comercial, 8h-18h), ou porque já é uma resposta esclarecendo uma
pergunta de ambiguidade anterior sua.

Responda SEMPRE E APENAS em JSON válido, numa única linha por valor, neste formato exato,
sem usar bloco de código markdown (nada de ```) e sem quebras de linha dentro dos valores. Inclua
TODAS as chaves sempre, mesmo vazias/false quando não se aplicarem:
{"eh_pedido_de_lembrete": true ou false, "destinatario_lembrete": "torres, luan ou tripa - quem deve receber o lembrete", "eh_recorrente": true ou false, "recorrencia_dia_mes": "dia do mes (1-31) se for recorrente mensal, ou string vazia", "data_hora_alvo_iso": "2026-08-29T15:00:00-03:00", "texto_lembrete": "um resumo curto e claro do que a pessoa quer ser lembrada de fazer", "eh_fato_para_lembrar": true ou false, "fato_texto": "o fato reescrito de forma clara e objetiva, ou string vazia", "eh_pergunta_sobre_grupo": true ou false, "grupo_perguntado": "nome do grupo mencionado, ou string vazia", "eh_pergunta_atividade_geral": true ou false, "eh_comando_para_tripa": true ou false, "mensagem_tripa": "texto pronto pra encaminhar pro grupo Tripa, ou string vazia", "tem_cobranca": true ou false, "horario_cobranca_iso": "horario ISO da cobranca, ou string vazia", "pergunta_cobranca": "pergunta curta pra mandar na cobranca, ou string vazia", "eh_comando_briefing_cliente": true ou false, "briefing_cliente_nome": "nome do cliente/grupo mencionado (ou inferido do contexto), ou string vazia", "briefing_assunto": "pista curta do assunto a analisar, ou string vazia", "eh_pergunta_metricool_metricas": true ou false, "metricool_metrica_cliente": "nome do cliente/marca, ou string vazia", "metricool_metrica_rede": "instagram ou facebook", "metricool_metrica_tipo": "seguidores, reels ou posts", "metricool_metrica_dias": 30, "resposta_conversa": "resposta natural pra mensagem, preenchida sempre que nenhum dos tipos 1/2/3/4/6/8/9 acima for verdadeiro"}
"""


# guarda no máximo 1 lembrete ativo por destinatario: {"torres": {...}, "luan": {...}, "tripa": {...}}
# - destinatario e quem deve RECEBER o lembrete, que pode ser diferente de quem pediu (ex:
# Torres pede pra lembrar o Luan de algo).
lembretes_ativos = {}


def enviar_lembrete(destinatario, numero_ou_jid, texto_lembrete, primeira_vez):
    prefixo = "⏰ Lembrete!" if primeira_vez else "⏰ Lembrete (ainda pendente):"
    enviar_texto(numero_ou_jid, f"{prefixo} {texto_lembrete}")


def agendar_nag(destinatario, numero_ou_jid, texto_lembrete):
    def checar_e_reenviar():
        info = lembretes_ativos.get(destinatario)
        if not info or info.get("resolvido"):
            return
        enviar_lembrete(destinatario, numero_ou_jid, texto_lembrete, primeira_vez=False)

    job = scheduler.add_job(checar_e_reenviar, "interval", minutes=30, id=f"nag-{destinatario}", replace_existing=True)
    return job


def agendar_lembrete(destinatario, numero_ou_jid, data_hora_alvo: datetime, texto_lembrete: str, repetir_ate_confirmar=True):
    """Agenda um lembrete pontual (uma unica data/hora). Pra Torres/Luan avisa 10 min antes e
    fica cobrando (nag a cada 30 min) ate a pessoa responder alguma coisa no privado. Pro
    grupo Tripa nao da pra saber quem "resolveu" a cobranca, entao manda so uma vez, na hora
    certa, sem ficar repetindo (igual a cobranca de comando pro Tripa que ja existia)."""
    agora_utc = datetime.now(timezone.utc)
    if repetir_ate_confirmar:
        aviso_em = data_hora_alvo - timedelta(minutes=10)
        if aviso_em <= agora_utc:
            aviso_em = agora_utc + timedelta(seconds=5)
        lembretes_ativos[destinatario] = {
            "texto": texto_lembrete,
            "alvo": data_hora_alvo.isoformat(),
            "resolvido": False,
        }
    else:
        aviso_em = data_hora_alvo if data_hora_alvo > agora_utc else agora_utc + timedelta(seconds=5)

    def disparar_primeiro_aviso():
        if repetir_ate_confirmar:
            info = lembretes_ativos.get(destinatario)
            if not info or info.get("resolvido"):
                return
            enviar_lembrete(destinatario, numero_ou_jid, texto_lembrete, primeira_vez=True)
            agendar_nag(destinatario, numero_ou_jid, texto_lembrete)
        else:
            enviar_lembrete(destinatario, numero_ou_jid, texto_lembrete, primeira_vez=True)

    scheduler.add_job(disparar_primeiro_aviso, "date", run_date=aviso_em, id=f"lembrete-{destinatario}-{int(time.time())}")


def agendar_lembrete_recorrente(destinatario, numero_ou_jid, dia_mes: int, hora: int, minuto: int, texto_lembrete: str, repetir_ate_confirmar=True):
    """Agenda um lembrete que se repete todo mes, num dia fixo (ex: dia 20), no horario
    informado (horario de Bahia - convertido pra UTC, que e o fuso do scheduler)."""
    hora_utc = (hora + 3) % 24  # Bahia = UTC-3 (sem horario de verao) -> UTC = Bahia + 3h

    def disparar():
        if repetir_ate_confirmar:
            lembretes_ativos[destinatario] = {
                "texto": texto_lembrete, "alvo": f"todo dia {dia_mes} do mes", "resolvido": False,
            }
            enviar_lembrete(destinatario, numero_ou_jid, texto_lembrete, primeira_vez=True)
            agendar_nag(destinatario, numero_ou_jid, texto_lembrete)
        else:
            enviar_lembrete(destinatario, numero_ou_jid, texto_lembrete, primeira_vez=True)

    scheduler.add_job(
        disparar, "cron", day=dia_mes, hour=hora_utc, minute=minuto,
        id=f"lembrete-recorrente-{destinatario}-{dia_mes}-{hora_utc}{minuto}",
        replace_existing=True,
    )


def resolver_destinatario_lembrete(destinatario_lembrete):
    """Traduz o campo 'destinatario_lembrete' do classificador (torres/luan/tripa) pra
    (chave interna, numero/jid de envio, nome de exibicao, se deve repetir cobrando ate
    confirmar). Devolve None quando o campo veio vazio/nao reconhecido, pra quem chamou usar
    o padrao (a propria pessoa que pediu o lembrete)."""
    d = normalizar_texto(destinatario_lembrete or "")
    if d == "luan":
        return "luan", LUAN_NUMBER, "o Luan", True
    if d == "torres":
        return "torres", TORRES_NUMBER, "o Torres", True
    if d == "tripa":
        return "tripa", TRIPA_DESIGNER_JID, "o grupo Tripa", False
    return None


def agendar_cobranca_tripa(horario_alvo: datetime, pergunta: str):
    """Agenda uma unica mensagem de cobranca pro grupo Tripa num horario especifico
    (ex: perguntar se um pedido com prazo ja foi feito)."""
    agora_utc = datetime.now(timezone.utc)
    if horario_alvo <= agora_utc:
        horario_alvo = agora_utc + timedelta(seconds=5)
    scheduler.add_job(
        lambda: enviar_texto(TRIPA_DESIGNER_JID, f"⏰ {pergunta}"),
        "date", run_date=horario_alvo, id=f"cobranca-tripa-{int(time.time())}",
    )


# Comandos "encaminhar pro Tripa" pedidos no privado passam por uma confirmacao antes de
# serem enviados de verdade - guarda no maximo 1 comando pendente por pessoa (torres/luan).
_comandos_pendentes = {}
_COMANDO_PENDENTE_TTL = 30 * 60  # descarta comando nao confirmado depois de 30 min

# Compromissos/promessas que a resposta automatica pareceu assumir com um cliente, aguardando
# confirmacao de Torres OU Luan (qualquer um dos dois responde) antes de virar um fato salvo.
_promessas_pendentes = []
_PROMESSA_PENDENTE_TTL = 30 * 60


def notificar_promessa_detectada(texto_promessa):
    """Quando a resposta automatica a um cliente pareceu assumir um compromisso concreto
    (prazo/acao), avisa Torres e Luan no privado perguntando se devem guardar isso de
    lembrete - assim nada fica prometido e esquecido só na conversa do grupo."""
    _promessas_pendentes.append({"texto": texto_promessa, "criado_em": time.time()})
    aviso = (
        f"🔔 Percebi um possível compromisso assumido com um cliente:\n\n\"{texto_promessa}\"\n\n"
        "Quer que eu guarde isso pra não esquecer? (responde \"sim\" ou \"não\")"
    )
    for numero in TEAM_NUMBERS:
        enviar_texto(numero, aviso)


def parece_confirmacao(texto: str):
    """Heuristica simples pra reconhecer sim/nao curtos, sem precisar chamar o Claude de
    novo so pra isso. Devolve True (confirma), False (cancela), ou None (ambiguo/nao e
    uma resposta de confirmacao, trata como mensagem nova)."""
    t = normalizar_texto(texto).strip()
    afirmativos = {"sim", "pode", "pode mandar", "confirma", "confirmado", "manda",
                   "isso mesmo", "isso", "ok", "beleza", "pode enviar", "manda sim"}
    negativos = {"nao", "cancela", "cancelar", "espera", "pera", "para", "deixa quieto"}
    if t in afirmativos:
        return True
    if t in negativos:
        return False
    return None


def marcar_resolvido(pessoa):
    info = lembretes_ativos.get(pessoa)
    if info and not info.get("resolvido"):
        info["resolvido"] = True
        try:
            scheduler.remove_job(f"nag-{pessoa}")
        except Exception:
            pass
        return True
    return False


SYSTEM_PROMPT_REVISAO = """Você é um revisor de texto cuidadoso da KingKong Filmes, revisando peças
gráficas (cards, artes, banners) e PDFs/informativos antes de irem ao ar. Leia TODO o texto visível
na imagem ou documento e identifique erros de ortografia, gramática/concordância, pontuação,
digitação (palavra trocada, faltando ou duplicada), e inconsistências óbvias de informação (ex:
dia da semana que não bate com a data, horário faltando, nome de pessoa/local escrito de duas
formas diferentes na mesma peça). NÃO opine sobre design, cores, layout ou estética - só sobre o
texto escrito. Seja rigoroso mas não invente erro que não existe; se estiver tudo certo, diga isso.

Responda SEMPRE E APENAS em JSON válido, sem bloco de código markdown (nada de ```):
{
  "tem_erro": true ou false,
  "erros": ["lista de erros encontrados, cada um curto e claro, citando o trecho exato que está errado e a correção"],
  "observacao": "qualquer observação adicional relevante (ex: texto ilegível em alguma parte), ou string vazia"
}
"""


def extrair_midia_para_revisao(key, data, message_type):
    """A partir de uma mensagem de imagem ou documento, baixa a midia e devolve
    (imagem_base64, pdf_base64, caption, aviso). 'aviso' e um texto pronto pra mandar
    de volta quando NAO da pra revisar (ex: nao baixou, ou nao e imagem/pdf); None se ok."""
    message = data.get("message", {})
    imagem_base64 = None
    pdf_base64 = None
    caption = ""
    aviso = None

    if "image" in message_type.lower():
        caption = message.get("imageMessage", {}).get("caption", "")
        imagem_base64 = baixar_midia_evolution(key)
        if not imagem_base64:
            aviso = "Recebi a imagem mas não consegui baixar pra revisar, pode reenviar?"
    elif "document" in message_type.lower():
        doc_msg = message.get("documentMessage", {})
        caption = doc_msg.get("caption", "")
        nome_arquivo = doc_msg.get("fileName", "arquivo.pdf")
        mimetype = doc_msg.get("mimetype", "")
        b64 = baixar_midia_evolution(key)
        if b64 and ("pdf" in mimetype.lower() or nome_arquivo.lower().endswith(".pdf")):
            pdf_base64 = b64
        elif b64:
            aviso = "Recebi o arquivo, mas só consigo revisar imagem (foto do card) ou PDF por enquanto."
        else:
            aviso = "Recebi o arquivo mas não consegui baixar pra revisar, pode reenviar?"
    else:
        aviso = "skip"  # tipo de mensagem que nem chega a ser imagem/documento

    return imagem_base64, pdf_base64, caption, aviso


def revisar_peca(imagem_base64, pdf_base64, caption):
    """Chama o Claude pra revisar a peca. Devolve (tem_erro, texto_formatado, resultado_bruto)."""
    prompt_usuario = "Revise essa peça em busca de erros de escrita." + (f" Legenda enviada junto: {caption}" if caption else "")
    resultado = chamar_claude(SYSTEM_PROMPT_REVISAO, prompt_usuario, imagem_base64=imagem_base64, pdf_base64=pdf_base64)

    if resultado.get("tem_erro"):
        erros = resultado.get("erros") or []
        texto_resp = "⚠️ Encontrei possíveis erros na peça:\n" + "\n".join(f"- {e}" for e in erros)
    else:
        texto_resp = "✅ Revisei e não encontrei erros de escrita. Está tudo certo!"
    if resultado.get("observacao"):
        texto_resp += f"\n\nObs: {resultado['observacao']}"

    return bool(resultado.get("tem_erro")), texto_resp, resultado


def revisar_arte_dm(numero, key, data, message_type):
    imagem_base64, pdf_base64, caption, aviso = extrair_midia_para_revisao(key, data, message_type)
    if aviso:
        enviar_texto(numero, aviso)
        return {"skipped": aviso}

    try:
        _, texto_resp, resultado = revisar_peca(imagem_base64, pdf_base64, caption)
    except Exception as e:
        enviar_texto(numero, "Tive um problema pra revisar esse arquivo agora, pode tentar de novo em instantes?")
        return {"erro_claude": str(e)}

    enviar_texto(numero, texto_resp)
    return {"resultado": resultado}


# --------------------------------------------------------------------------
# Comparacao da arte finalizada com o pedido original do cliente
# --------------------------------------------------------------------------
#
# Quando um pedido de arte e encaminhado pra Tripa Designer, guardamos o pedido
# organizado numa fila em memoria, por cliente. Quando o designer posta a arte
# pronta no grupo Tripa Designer citando o nome do cliente na legenda (ex:
# "terapia"), pegamos o pedido mais antigo pendente daquele cliente e pedimos
# pro Claude comparar se a arte contempla tudo que foi pedido.

_pedidos_pendentes_designer = {}  # chave normalizada do cliente -> lista de pedidos (FIFO)
_pedidos_lock = threading.Lock()
_PEDIDO_PENDENTE_TTL = 7 * 24 * 60 * 60  # descarta pedido nao reclamado depois de 7 dias


def normalizar_texto(txt):
    txt = (txt or "").lower()
    txt = unicodedata.normalize("NFKD", txt).encode("ascii", "ignore").decode("ascii")
    return txt


def registrar_pedido_pendente(cliente_nome, pedido_texto, grupo_jid=None, tipo_peca=None):
    """Registra um pedido de arte aguardando entrega, pra comparar depois com a arte
    finalizada. Usa o banco de dados (tabela tarefas) quando disponivel, pra sobreviver a
    reinicios/redeploys do servico; se o banco nao estiver configurado (ou der erro), cai
    pro fallback antigo em memoria (que se perde se o servico reiniciar nesse meio tempo)."""
    if DATABASE_URL:
        tarefa_id = criar_tarefa(cliente_nome, grupo_jid, tipo_peca, pedido_texto, autor="cliente")
        if tarefa_id is not None:
            return
        print("[registrar_pedido_pendente] banco de dados falhou, usando fallback em memoria", flush=True)
    chave = normalizar_texto(cliente_nome)
    with _pedidos_lock:
        fila = _pedidos_pendentes_designer.setdefault(chave, [])
        fila.append({
            "cliente_nome": cliente_nome, "pedido_texto": pedido_texto,
            "grupo_jid": grupo_jid, "timestamp": time.time(),
        })


_STOPWORDS_NOME_CLIENTE = {"de", "do", "da", "e", "dr", "dra", "grupo"}


def extrair_cliente_da_legenda(caption):
    """Se a legenda citar o nome de algum cliente conhecido (GRUPOS, exceto grupos
    internos), devolve o nome oficial do grupo. Senao, devolve None."""
    if not caption:
        return None
    cap_norm = normalizar_texto(caption)
    for grupo in GRUPOS.values():
        if grupo["interno"]:
            continue
        nome_norm = normalizar_texto(grupo["nome"])
        if nome_norm and nome_norm in cap_norm:
            return grupo["nome"]
        for palavra in nome_norm.split():
            if len(palavra) >= 4 and palavra not in _STOPWORDS_NOME_CLIENTE and palavra in cap_norm:
                return grupo["nome"]
    return None


def buscar_pedido_pendente(cliente_nome):
    """Pega o pedido pendente mais antigo daquele cliente - do banco de dados quando
    disponivel (nesse caso a tarefa fica aberta, permitindo varias rodadas de correcao
    ate ser marcada concluida), ou da fila em memoria como fallback (nesse caso o pedido
    e removido da fila ao ser encontrado, e pedidos vencidos pelo TTL sao descartados).
    Devolve um dict com pelo menos as chaves 'pedido_texto' e 'cliente_nome', e 'tarefa_id'
    preenchido quando veio do banco (None quando veio do fallback em memoria); ou None se
    nao tiver nenhum pedido pendente daquele cliente."""
    if DATABASE_URL:
        tarefa = buscar_tarefa_pendente_por_cliente(cliente_nome)
        if tarefa:
            return {
                "tarefa_id": tarefa["id"],
                "cliente_nome": tarefa["cliente_nome"],
                "pedido_texto": tarefa["descricao"],
                "grupo_jid": tarefa.get("grupo_jid"),
            }
        return None
    chave = normalizar_texto(cliente_nome)
    agora = time.time()
    with _pedidos_lock:
        fila = _pedidos_pendentes_designer.get(chave)
        if not fila:
            return None
        fila[:] = [p for p in fila if agora - p["timestamp"] <= _PEDIDO_PENDENTE_TTL]
        if not fila:
            return None
        pedido = fila.pop(0)
        pedido["tarefa_id"] = None
        return pedido


SYSTEM_PROMPT_COMPARACAO_PEDIDO = """Você faz a CONFERÊNCIA DE CONTEÚDO de uma peça gráfica finalizada
antes dela sair pro cliente - essa conferência é uma camada de qualidade separada da revisão de
português, e existe SÓ internamente (nunca é vista pelo cliente). Você não está aqui pra corrigir
ortografia (isso já é feito em outra etapa) - você está aqui pra checar se a INFORMAÇÃO da arte
bate exatamente com o que o cliente pediu de verdade, na versão mais atual/confirmada do pedido.

FONTE DA VERDADE: o "pedido original organizado" que você vai receber já é a versão consolidada do
pedido no momento em que foi encaminhado pro designer. Mas, junto com ele, você também pode receber
um HISTÓRICO RECENTE DO CLIENTE (mensagens de texto, áudio já transcrito, e observações da equipe)
que pode conter CORREÇÕES OU ALTERAÇÕES enviadas DEPOIS que o pedido já tinha sido encaminhado pro
designer (pelo próprio cliente, por Torres ou por Luan) - texto e áudio têm o MESMO peso, nunca
ignore uma alteração só porque veio por áudio. Se o histórico mostrar uma informação mais recente
que contradiz o pedido original organizado (ex: pedido dizia "R$ 37,90" mas depois alguém confirmou
"pode manter R$ 39,90 mesmo"), a informação MAIS RECENTE E CONFIRMADA é que vale - reconstrua
mentalmente esse "briefing final" antes de comparar com a arte. Nunca use só a primeira mensagem
nem ignore uma correção posterior.

O QUE CONFERIR (compare a arte, item por item, com o briefing final reconstruído): nome do
cliente/empresa/produto/pessoa, valores (preço original e promocional), datas, dias da semana,
horários, condições, exceções/restrições (ex: "exceto tal item"), endereço, telefone, e qualquer
outra informação concreta que o cliente forneceu.

REGRAS DE PRECISÃO (não flexibilize nenhuma delas):
- Valores: compare o número exato. R$ 39,90 e R$ 39,99 são DIFERENTES - isso é erro, não uma
  escolha estética do designer. Nunca arredonde, nunca trate como equivalente.
- Nomes próprios: compare a grafia exata contra o que está confirmado no histórico/pedido (ex:
  "Terapia Beach" vs "Terapia Bech", ou "Fellipe" vs "Felipe" - mesmo uma letra de diferença é erro).
- Datas/dias da semana/horários: confira dia, dia da semana e horário um a um.
- Exceções e condições (ex: "exceto salmão e moqueca"): se o pedido exigia essa informação e ela
  não aparece na arte, isso é uma falha de conteúdo mesmo que o resto (preço, título) esteja certo -
  nunca aprove só porque a parte principal bateu.
- NUNCA aprove só pela aparência/design da arte estar bonita - sempre extraia as informações da
  arte e compare com o briefing final reconstruído antes de decidir.

QUANDO HOUVER DÚVIDA (ex: uma transcrição de áudio no histórico não ficou clara sobre qual valor
vale, ou duas mensagens parecem se contradizer sem dar pra saber qual é a mais recente/confirmada):
NÃO escolha uma das opções por conta própria e não invente. Marque "duvida_ambigua" como true e
preencha "pergunta_duvida" com uma pergunta objetiva pra Torres ou Luan resolverem, citando as duas
informações em conflito e o cliente/peça em questão.

Responda SEMPRE E APENAS em JSON válido, sem bloco de código markdown (nada de ```):
{
  "bate_com_pedido": true ou false,
  "problemas": ["lista de itens faltando ou diferentes do briefing final, cada um citando o valor/informação pedida e o que está na arte"],
  "resumo": "1 frase confirmando que bateu tudo, ou resumindo o principal problema",
  "duvida_ambigua": true ou false,
  "pergunta_duvida": "pergunta objetiva pra Torres/Luan quando houver conflito não resolvido no histórico, ou string vazia"
}
"""


def comparar_arte_com_pedido(pedido_texto, imagem_base64, pdf_base64, historico_texto=""):
    bloco_historico = (
        f"\n\nHISTÓRICO RECENTE DO CLIENTE (mais antigo primeiro - pode conter correções enviadas "
        f"DEPOIS do pedido original acima; a informação mais recente e confirmada é que vale):\n{historico_texto}"
    ) if historico_texto else ""
    prompt_usuario = (
        f"Pedido original organizado (no momento em que foi encaminhado pro designer):\n{pedido_texto}"
        f"{bloco_historico}\n\nFaça a conferência de conteúdo dessa arte anexada contra o briefing final."
    )
    resultado = chamar_claude(SYSTEM_PROMPT_COMPARACAO_PEDIDO, prompt_usuario, imagem_base64=imagem_base64, pdf_base64=pdf_base64)

    if resultado.get("duvida_ambigua"):
        bate = False
        texto_resp = ""  # a duvida vai só pro privado de Torres/Luan, nunca pro grupo Tripa como se fosse erro confirmado
        return bate, texto_resp, resultado

    bate = bool(resultado.get("bate_com_pedido"))
    if bate:
        texto_resp = f"✅ Conferi com o pedido do cliente: {resultado.get('resumo') or 'bateu tudo certo, contempla o que foi pedido.'}"
    else:
        problemas = resultado.get("problemas") or []
        texto_resp = "⚠️ Comparei com o pedido do cliente e encontrei diferença(s):\n" + "\n".join(f"- {p}" for p in problemas)
        if resultado.get("resumo"):
            texto_resp += f"\n\n{resultado['resumo']}"

    return bate, texto_resp, resultado


def _extrair_texto_log_tripa(data, message_type):
    """Extrai um texto simples pra registrar no historico do grupo Tripa, cobrindo os
    tipos de mensagem mais comuns (texto, imagem/documento/video com ou sem legenda,
    audio). Devolve None quando nao ha nada relevante pra logar (ex: reacao, sticker) -
    isso e so pra guardar o historico, nao interfere na logica de revisao de peca."""
    message = data.get("message", {})
    tipo = (message_type or "").lower()
    if message.get("conversation"):
        return message.get("conversation")
    if message.get("extendedTextMessage", {}).get("text"):
        return message["extendedTextMessage"]["text"]
    if "image" in tipo:
        caption = message.get("imageMessage", {}).get("caption", "")
        return f"[imagem] {caption}" if caption else "[imagem enviada]"
    if "document" in tipo:
        caption = message.get("documentMessage", {}).get("caption", "")
        return f"[arquivo] {caption}" if caption else "[arquivo enviado]"
    if "video" in tipo:
        caption = message.get("videoMessage", {}).get("caption", "")
        return f"[vídeo] {caption}" if caption else "[vídeo enviado]"
    if "audio" in tipo:
        return "[áudio enviado]"
    return None


def processar_revisao_grupo_designer(remote_jid, key, data):
    """No grupo Tripa Designer: se alguem postar uma foto/PDF de peca, revisa a ortografia
    e SEMPRE avisa no grupo o resultado (bateu ou nao bateu). Se a legenda citar o nome de
    um cliente (ex: "terapia") e tiver um pedido de arte pendente daquele cliente, TAMBEM
    compara a arte com o pedido original e avisa o resultado dessa comparação também."""
    message_type = data.get("messageType", "")

    # Registra TUDO que acontece no grupo Tripa (texto, imagem, arquivo, video, audio) no
    # historico, mesmo mensagens que nao viram revisao de peca - assim da pra perguntar
    # depois no privado "o que rolou no Tripa" e a Cintia sabe responder.
    conteudo_log_tripa = _extrair_texto_log_tripa(data, message_type)
    if conteudo_log_tripa:
        registrar_mensagem_grupo(
            remote_jid, GRUPOS.get(remote_jid, {}).get("nome", "Tripa"),
            data.get("pushName", "equipe"), conteudo_log_tripa, True,
        )

    imagem_base64, pdf_base64, caption, aviso = extrair_midia_para_revisao(key, data, message_type)
    if aviso:
        # No grupo nao mandamos os avisos de "nao consegui baixar" pra nao gerar ruido -
        # so logamos e seguimos.
        print(f"[processar_revisao_grupo_designer] {aviso}", flush=True)
        return {"skipped": aviso}

    try:
        tem_erro, texto_resp, resultado = revisar_peca(imagem_base64, pdf_base64, caption)
    except Exception as e:
        print(f"[processar_revisao_grupo_designer] erro claude: {e}", flush=True)
        return {"erro_claude": str(e)}

    enviar_texto(remote_jid, texto_resp)

    resultado_comparacao = None
    cliente_nome = extrair_cliente_da_legenda(caption)
    if cliente_nome:
        pedido = buscar_pedido_pendente(cliente_nome)
        if pedido:
            # Busca o historico recente da conversa do cliente (grupo dele) - pode conter
            # correcoes/alteracoes enviadas DEPOIS que o pedido ja tinha sido encaminhado pro
            # designer (por texto ou audio, do cliente ou da equipe), que o "pedido_texto"
            # congelado sozinho nao capturaria. Isso e o que permite reconstruir o briefing
            # final de verdade antes de conferir a arte, em vez de comparar so com a primeira
            # versao do pedido.
            historico_texto = ""
            grupo_jid_cliente = pedido.get("grupo_jid")
            if grupo_jid_cliente:
                historico_cliente = buscar_mensagens_recentes_grupo(grupo_jid_cliente, limite=20)
                historico_texto = "\n".join(f"- {m['autor']}: {m['conteudo']}" for m in historico_cliente)
            try:
                bate, texto_comparacao, resultado_comparacao = comparar_arte_com_pedido(
                    pedido["pedido_texto"], imagem_base64, pdf_base64, historico_texto=historico_texto
                )
            except Exception as e:
                print(f"[processar_revisao_grupo_designer] erro na comparacao com pedido: {e}", flush=True)
            else:
                if resultado_comparacao.get("duvida_ambigua"):
                    # Nunca escolhe uma informacao em duvida por conta propria - so pergunta pra
                    # Torres/Luan em privado, sem postar nenhum veredito (certo/errado) no Tripa
                    # enquanto a duvida nao for resolvida por um humano.
                    pergunta = resultado_comparacao.get("pergunta_duvida") or (
                        f"Estou conferindo uma arte do cliente {cliente_nome} e encontrei uma "
                        "informação que não consegui confirmar com segurança no histórico. Pode "
                        "dar uma olhada?"
                    )
                    for numero in TEAM_NUMBERS:
                        enviar_texto(numero, f"❓ Dúvida na conferência de conteúdo ({cliente_nome}):\n\n{pergunta}")
                else:
                    enviar_texto(remote_jid, texto_comparacao)
                    # Quando o pedido veio do banco (tem tarefa_id), atualiza o status da
                    # tarefa de acordo com o resultado da conferencia: concluida se bateu
                    # tudo certo, ou aguardando correcao se faltou/tem algo errado - assim a
                    # mesma tarefa continua aberta pra receber a proxima rodada corrigida.
                    if pedido.get("tarefa_id"):
                        novo_status = STATUS_CONCLUIDO if bate else STATUS_AGUARDANDO_CORRECAO
                        adicionar_evento_tarefa(
                            pedido["tarefa_id"], "entrega", texto_comparacao,
                            autor="designer", novo_status=novo_status,
                        )
        else:
            print(
                f"[processar_revisao_grupo_designer] legenda citou '{cliente_nome}' mas nao "
                "achei pedido pendente pra comparar",
                flush=True,
            )

    return {"resultado": resultado, "cliente_identificado": cliente_nome, "comparacao": resultado_comparacao}


SYSTEM_PROMPT_CORRECAO_CONSERVADORA = """Você corrige um texto em português do Brasil que a pessoa
escreveu com dificuldade. IMPORTANTE - aqui você NÃO reescreve nem melhora o texto, só corrige o
que está objetivamente errado:

- Corrija ortografia, acentuação, pontuação, concordância verbal/nominal e erros de digitação.
- NUNCA troque palavras por sinônimos, nunca deixe o texto mais formal, nunca mude o tom, nunca
  resuma, nunca acrescente nem remova informação, nunca reorganize o raciocínio ou a ordem das
  ideias. Mantenha as mesmas palavras sempre que possível.
- O resultado deve parecer escrito pela MESMA pessoa, do mesmo jeito, só sem erro - alguém que
  conhece quem escreveu não pode notar que "não parece mais ela/ele".
- Preserve tratamentos informais, cumprimentos ("Amiga", "Bom dia" etc.) e a pontuação emocional
  (like "!", "?") do jeito que a pessoa usou, corrigindo só o que está de fato incorreto.

Responda SEMPRE E APENAS em JSON válido, sem texto fora do JSON e sem bloco de código markdown
(nada de ```), neste formato exato:
{"texto_corrigido": "o texto corrigido, preservando ao máximo as palavras e o estilo original"}
"""

SYSTEM_PROMPT_CORRECAO_TEXTO = """Você ajuda a revisar e reescrever, em português do Brasil, um texto
que a pessoa escreveu com dificuldade (pode ter erro de ortografia, gramática, concordância, ou
frases desorganizadas/informais demais). A pessoa pediu explicitamente pra reescrever no tom
"{tom}" (isso é diferente de uma simples correção - ela quer uma versão diferente, não só sem erro).

- Corrija toda a ortografia, gramática e concordância.
- Reescreva no tom pedido: "formal" é mais sério e profissional, sem gírias e sem emojis;
  "cordial" é educado, caloroso e amigável, mas ainda soa natural (pode usar 1 emoji se fizer
  sentido no contexto).
- Preserve o significado e a intenção original do texto - não invente informação nova nem mude o
  que a pessoa quis dizer.
- Gere 2 opções DIFERENTES entre si (não apenas uma troca mínima de palavra), pra pessoa escolher
  a que soa melhor pra ela.

Responda SEMPRE E APENAS em JSON válido, sem texto fora do JSON e sem bloco de código markdown
(nada de ```), neste formato exato:
{"opcao_1": "primeira versão reescrita e corrigida", "opcao_2": "segunda versão reescrita e corrigida, diferente da primeira"}
"""

# Corrigir/corrige/revisa sozinho (sem pedir reescrita) = so conserta erro, preservando as
# palavras - reescrita completa (outro tom, mais formal, etc) só quando pedida explicitamente.
_REGEX_PEDIDO_CORRECAO = re.compile(r"^\s*(?:corri[gj]\w*|revis\w*)\b.*?:", re.IGNORECASE | re.DOTALL)
_PALAVRAS_PEDEM_REESCRITA = (
    "formal", "cordial", "profissional", "comercial", "melhora", "melhore", "melhorar",
    "reescreve", "reescreva", "reescrever", "outra versao", "outra opcao", "resuma", "resumir",
)


def extrair_modo_e_texto_correcao(texto_completo):
    """Se a mensagem começar com 'corrija'/'corrige'/'revise' seguido de ':', devolve
    (modo, texto_a_corrigir). "modo" e "conservador" (so ortografia/gramatica/pontuacao,
    preservando as palavras) quando a pessoa so pediu correcao mesmo, sem mencionar tom
    ou reescrita - esse e o padrao, igual um "corrija" simples deve funcionar. So vira
    "formal"/"cordial" (reescrita completa com 2 opcoes) quando ela pede isso explicitamente
    (ex: "corrija com tom formal", "corrija e deixa mais profissional", "reescreve isso")."""
    m = _REGEX_PEDIDO_CORRECAO.match(texto_completo)
    if not m:
        return None, None
    prefixo = normalizar_texto(m.group(0))
    texto_a_corrigir = texto_completo[m.end():].strip()
    if "formal" in prefixo:
        return "formal", texto_a_corrigir
    if "cordial" in prefixo:
        return "cordial", texto_a_corrigir
    if any(palavra in prefixo for palavra in _PALAVRAS_PEDEM_REESCRITA):
        return "cordial", texto_a_corrigir
    return "conservador", texto_a_corrigir


def corrigir_texto_dm(numero, modo, texto_a_corrigir):
    if not texto_a_corrigir:
        enviar_texto(
            numero,
            "Manda o texto depois dos dois pontos, tipo: \"Corrija: <seu texto aqui>\" (só "
            "corrige o erro) ou \"Corrija com tom formal: <seu texto aqui>\" (se quiser uma "
            "versão reescrita)",
        )
        return {"skipped": "pedido de correcao sem texto"}

    pessoa = "torres" if numero_bate(numero, TORRES_NUMBER) else "luan"
    grupo_jid_dm = f"dm_{pessoa}"
    grupo_nome_dm = "Privado - Torres" if pessoa == "torres" else "Privado - Luan"

    prompt_sistema = (
        SYSTEM_PROMPT_CORRECAO_CONSERVADORA if modo == "conservador"
        else SYSTEM_PROMPT_CORRECAO_TEXTO.replace("{tom}", modo)
    )
    try:
        resultado = chamar_claude(prompt_sistema, texto_a_corrigir)
    except Exception as e:
        enviar_texto(numero, "Tive um problema pra revisar esse texto agora, pode tentar de novo?")
        return {"erro_claude": str(e)}

    if modo == "conservador":
        resposta = resultado.get("texto_corrigido", "")
    else:
        opcao_1 = resultado.get("opcao_1", "")
        opcao_2 = resultado.get("opcao_2", "")
        resposta = f"Aqui vão 2 opções no tom {modo}:\n\n1️⃣ {opcao_1}\n\n2️⃣ {opcao_2}"

    enviar_texto(numero, resposta)
    # Registra a resposta no historico do DM tambem, pra se a pessoa disser depois "manda
    # só a segunda"/"gostei dessa" o bot conseguir recuperar o que foi mostrado, em vez de
    # inventar outra coisa.
    registrar_mensagem_grupo(grupo_jid_dm, grupo_nome_dm, "Cintia", resposta, False)
    return {"resultado": resultado}


SYSTEM_PROMPT_CONSULTA_GRUPO = """Você é a Cintia, assistente virtual da Correria. {pessoa_nome} te
perguntou algo no privado sobre o grupo de WhatsApp "{grupo_nome}", pra não precisar abrir o grupo
pra conferir. Abaixo está o histórico recente de mensagens desse grupo que você tem disponível
(pode ser que não cubra tudo, só o que foi registrado).

Responda a pergunta de {pessoa_nome} usando SOMENTE essas mensagens como base. Se a resposta não
estiver clara nas mensagens disponíveis, diga isso com naturalidade (ex: "não tenho esse
registro/isso não apareceu no que eu vi do grupo") em vez de inventar. Responda de forma natural,
objetiva e humanizada, como uma colega de equipe contando o que viu.

HISTÓRICO DO GRUPO:
{historico}

Responda SEMPRE E APENAS em JSON válido, sem bloco de código markdown (nada de ```):
{"resposta": "sua resposta natural pra {pessoa_nome}"}
"""


def responder_pergunta_sobre_grupo(pessoa_nome, pergunta, grupo_jid, grupo_nome):
    mensagens = buscar_mensagens_recentes_grupo(grupo_jid)
    if not mensagens:
        return f"Ainda não tenho nenhum histórico registrado do grupo \"{grupo_nome}\" pra consultar."
    linhas = []
    for m in mensagens:
        quem = "equipe" if m.get("eh_equipe") else (m.get("autor") or "cliente")
        linhas.append(f"- {quem}: {m.get('conteudo', '')}")
    historico = "\n".join(linhas)
    prompt_sistema = (
        SYSTEM_PROMPT_CONSULTA_GRUPO
        .replace("{pessoa_nome}", pessoa_nome)
        .replace("{grupo_nome}", grupo_nome)
        .replace("{historico}", historico)
    )
    try:
        resultado = chamar_claude(prompt_sistema, pergunta)
        return resultado.get("resposta") or "Não consegui montar uma resposta a partir do histórico do grupo, pode reformular a pergunta?"
    except Exception as e:
        print(f"[responder_pergunta_sobre_grupo] erro: {e}", flush=True)
        return "Tive um problema pra consultar o histórico desse grupo agora, tenta de novo daqui a pouco?"


SYSTEM_PROMPT_BRIEFING_CLIENTE = """Você é a Cintia, assistente virtual da Correria. {pessoa_nome} te
pediu pra analisar a conversa do cliente "{grupo_nome}" e transformar o que foi decidido num
briefing pronto pra encaminhar pro designer (grupo Criações/Gravações • Tripa • Correria).

Você NÃO deve usar só a última mensagem, nem só a primeira - use TODO o histórico abaixo como
memória, mas foque no assunto pedido (se {pessoa_nome} mencionou um assunto específico: "{assunto}").
Ignore partes do histórico que claramente sejam sobre outro assunto sem relação nenhuma com esse
pedido (ex: uma conversa antiga sobre um evento diferente).

COMO RECONSTRUIR O ESTADO FINAL (muito importante): uma solicitação pode evoluir ao longo da
conversa - o cliente pede algo, depois manda um áudio corrigindo um valor, a equipe complementa,
o cliente confirma ou corrige de novo. Áudio (já vem transcrito no histórico) tem o MESMO peso que
texto - nunca ignore uma alteração só porque veio por áudio. A informação MAIS RECENTE E CONFIRMADA
sempre vale sobre a mais antiga (ex: "Brahma" depois "melhor Heineken" depois "volta pra Brahma" =
o estado final é Brahma). Respostas curtas como "isso", "esses", "pode", "sim" só fazem sentido
lidas junto com a mensagem anterior a que respondem - use o histórico pra entender a que elas se
referem, nunca comente sobre elas soltas.

O QUE CADA BRIEFING PRECISA TER (sempre que existir na conversa): produto/material, objetivo,
formato da peça, texto/título, valores, datas, dias da semana, horários, nomes próprios, condições,
exceções/restrições, referências (ex: "usar a arte anterior como referência visual"), e qualquer
outra informação concreta que o cliente ou a equipe confirmaram. O designer precisa conseguir
trabalhar SÓ com o briefing, sem precisar voltar no grupo do cliente - transforme a conversa em
decisões organizadas, não narre a conversa inteira (nunca escreva algo como "primeiro o cliente
disse X, depois mandou áudio, depois Torres respondeu..." a menos que isso seja realmente
necessário pra entender uma mudança importante).

UMA CONVERSA PODE TER MAIS DE UM PEDIDO (muito importante - NUNCA assuma "uma conversa = um
pedido"): antes de montar o(s) briefing(s), pergunte-se internamente se existe UMA solicitação ou
VÁRIAS solicitações diferentes na conversa analisada. O critério é o OBJETIVO/ENTREGA: se duas
partes da conversa pedem materiais ou ações diferentes (ex: atualizar a arte da promoção do prato
executivo E incluir um artista na programação de sábado), são DOIS pedidos separados, mesmo que
tenham sido mencionados na mesma conversa ou até na mesma leva de mensagens - NUNCA misture os dois
num briefing só. Já um COMPLEMENTO (ex: "inclui Roberto Neves sábado" seguido de "às 15h30") ou uma
CORREÇÃO (ex: "às 15h30" depois "na verdade 16h") do mesmo objetivo continuam sendo o MESMO pedido -
NUNCA crie um pedido novo pra cada complemento/correção, sempre absorva no pedido que já existe e
mantenha só o estado final. Preste atenção especial em pedidos curtos/rápidos no MEIO ou no FIM de
uma conversa longa sobre outro assunto (ex: uma mensagem pequena e direta depois de várias mensagens
sobre um assunto totalmente diferente) - NUNCA deixe passar um pedido só porque ele é curto ou veio
"escondido" no meio/fim de uma conversa mais longa sobre outra coisa.

SE HOUVER CONFLITO/DÚVIDA REAL EM ALGUM PEDIDO: se encontrar duas informações que parecem se
contradizer e não der pra saber com segurança qual é a mais recente/confirmada (ex: um valor no
texto e outro numa transcrição de áudio, sem deixar claro qual venceu; ou não souber com segurança
qual material/arte antiga um pedido de alteração se refere), NÃO escolha um dos dois por conta
própria e não invente. Marque "duvida_ambigua" como true e preencha "pergunta_duvida" com uma
pergunta objetiva pra {pessoa_nome} resolver, citando o pedido e as informações em conflito. Nesse
caso pode deixar "pedidos" vazio ou parcial - a dúvida é resolvida antes de qualquer coisa ser
encaminhada.

SE NÃO ENCONTRAR NADA RELACIONADO AO ASSUNTO PEDIDO: marque "duvida_ambigua" como true e explique
em "pergunta_duvida" que não achou uma solicitação clara sobre esse assunto nessa conversa.

Responda SEMPRE E APENAS em JSON válido, sem bloco de código markdown (nada de ```):
{
  "duvida_ambigua": true ou false,
  "pergunta_duvida": "pergunta objetiva pra {pessoa_nome} quando houver conflito ou nada encontrado, ou string vazia",
  "resumo_curto": "1 frase curta resumindo quantos pedidos foram identificados e o que vai ser encaminhado, pra {pessoa_nome} confirmar antes de mandar",
  "pedidos": [
    {
      "titulo": "nome curto do pedido (ex: 'Prato Executivo', 'Programação de sábado')",
      "tipo_peca": "classificação curta do material (Card, Story, Carrossel, Banner, Flyer, Selo...), ou string vazia se não for uma peça gráfica",
      "descricao_briefing": "as informações finais confirmadas desse pedido específico, organizadas em lista clara e objetiva pro designer, já considerando todas as alterações/correções da conversa"
    }
  ]
}
Inclua em "pedidos" UM item pra cada solicitação distinta que você identificou (pode ser 1 item, ou
vários) - nunca junte pedidos com objetivos diferentes no mesmo item da lista.
"""


def montar_briefing_cliente(pessoa_nome, grupo_jid, grupo_nome, assunto):
    """Analisa o historico da conversa de UM cliente (nao so a ultima mensagem) e monta um
    briefing consolidado pronto pra mandar pro Tripa - reconstruindo o estado final do pedido
    a partir de toda a evolucao da conversa (correcoes por texto E audio incluidas), em vez de
    usar so a primeira ou a ultima mensagem sobre o assunto."""
    mensagens = buscar_mensagens_recentes_grupo(grupo_jid, limite=60)
    if not mensagens:
        return {"duvida_ambigua": True, "pergunta_duvida": f"Ainda não tenho nenhum histórico registrado do grupo \"{grupo_nome}\" pra montar um briefing."}
    historico = "\n".join(f"- {m['autor']}: {m['conteudo']}" for m in mensagens)
    prompt_sistema = (
        SYSTEM_PROMPT_BRIEFING_CLIENTE
        .replace("{pessoa_nome}", pessoa_nome)
        .replace("{grupo_nome}", grupo_nome)
        .replace("{assunto}", assunto or "(não especificado - use a solicitação mais recente/relevante)")
    )
    try:
        return chamar_claude(prompt_sistema, f"HISTÓRICO DA CONVERSA DO CLIENTE:\n{historico}")
    except Exception as e:
        print(f"[montar_briefing_cliente] erro: {e}", flush=True)
        return {"duvida_ambigua": True, "pergunta_duvida": "Tive um problema técnico tentando montar esse briefing, pode pedir de novo daqui a pouco?"}


SYSTEM_PROMPT_ATIVIDADE_GERAL = """Você é a Cintia, assistente virtual da Correria. {pessoa_nome}
perguntou quais grupos (de cliente ou o grupo interno Tripa) tiveram atividade hoje, pra ter um
panorama geral sem precisar abrir grupo por grupo. Abaixo está, pra cada grupo que teve pelo menos
uma mensagem hoje, um trecho do que foi registrado.

Pra CADA grupo listado, escreva um resumo BEM curto (uma frase só) do que rolou lá, com base
SOMENTE nas mensagens mostradas - sem inventar nada que não esteja ali. Se o grupo só teve
mensagem da própria equipe (sem nada de cliente), pode dizer isso também.

ATIVIDADE DE HOJE POR GRUPO:
{blocos_grupos}

Responda SEMPRE E APENAS em JSON válido, sem bloco de código markdown (nada de ```), neste
formato exato:
{"resumos": [{"grupo": "nome do grupo", "resumo": "resumo de uma frase do que rolou nesse grupo"}]}
"""


def responder_atividade_geral_hoje(pessoa_nome):
    grupos_ativos = listar_grupos_com_atividade_hoje()
    if not grupos_ativos:
        return "Hoje ainda não teve nenhuma atividade registrada em nenhum grupo (nem de cliente, nem no Tripa)."

    blocos = []
    for info in grupos_ativos.values():
        linhas = []
        for m in info["mensagens"][-15:]:
            quem = "equipe" if m.get("eh_equipe") else (m.get("autor") or "cliente")
            linhas.append(f"  - {quem}: {m.get('conteudo', '')}")
        blocos.append(f"Grupo \"{info['nome']}\":\n" + "\n".join(linhas))
    blocos_grupos = "\n\n".join(blocos)

    prompt_sistema = (
        SYSTEM_PROMPT_ATIVIDADE_GERAL
        .replace("{pessoa_nome}", pessoa_nome)
        .replace("{blocos_grupos}", blocos_grupos)
    )
    try:
        resultado = chamar_claude(prompt_sistema, "Quais grupos tiveram atividade hoje?")
        resumos = resultado.get("resumos") or []
        if not resumos:
            nomes = ", ".join(info["nome"] for info in grupos_ativos.values())
            return f"Hoje tiveram atividade: {nomes}."
        linhas_resposta = [f"• *{r.get('grupo', '')}*: {r.get('resumo', '')}" for r in resumos]
        return "Hoje tiveram atividade nesses grupos:\n\n" + "\n".join(linhas_resposta)
    except Exception as e:
        print(f"[responder_atividade_geral_hoje] erro: {e}", flush=True)
        nomes = ", ".join(info["nome"] for info in grupos_ativos.values())
        return f"Tive um problema pra resumir, mas hoje tiveram atividade nesses grupos: {nomes}."


# --------------------------------------------------------------------------
# Parte 6: integracao com o Metricool (so no privado de Torres/Luan)
# --------------------------------------------------------------------------
#
# Duas funcoes: (1) consultar metricas de alguma marca/cliente cadastrado no
# Metricool, e (2) agendar um post (feed/story/reel) a partir de um link publico
# (Google Drive ou outro), do jeito que Torres/Luan ja fazem hoje conversando com
# o Claude - so que agora pelo WhatsApp. NUNCA disponivel pra clientes, so
# reconhecido dentro de processar_dm (numero ja verificado como Torres ou Luan).

_marcas_metricool_cache = {"dados": None, "buscado_em": 0}
_MARCAS_METRICOOL_CACHE_TTL = 15 * 60  # 15 min - marca nova cadastrada demora no maximo isso pra aparecer


def metricool_headers():
    return {"X-Mc-Auth": METRICOOL_USER_TOKEN, "Content-Type": "application/json"}


def metricool_listar_marcas(forcar=False):
    """Lista as marcas/clientes cadastrados no Metricool (id + nome), com cache em
    memoria de 15 min pra nao bater na API a cada mensagem. Se a chamada falhar,
    devolve o cache antigo (se tiver) em vez de quebrar tudo."""
    agora = time.time()
    if not forcar and _marcas_metricool_cache["dados"] is not None and (agora - _marcas_metricool_cache["buscado_em"]) < _MARCAS_METRICOOL_CACHE_TTL:
        return _marcas_metricool_cache["dados"]
    try:
        resp = requests.get(
            f"{METRICOOL_BASE_URL}/admin/simpleProfiles",
            headers=metricool_headers(),
            params={"userId": METRICOOL_USER_ID},
            timeout=15,
        )
        resp.raise_for_status()
        body = resp.json()
        marcas = body.get("data", body) if isinstance(body, dict) else body
        _marcas_metricool_cache["dados"] = marcas
        _marcas_metricool_cache["buscado_em"] = agora
        return marcas
    except Exception as e:
        print(f"[metricool_listar_marcas] erro ao buscar marcas: {e}", flush=True)
        return _marcas_metricool_cache["dados"] or []


def metricool_identificar_marca(texto):
    """Acha, pelo nome mencionado na mensagem, a marca/cliente correspondente no
    Metricool (comparacao sem acento/case, por substring - mesmo criterio usado
    pra identificar grupo de cliente). Devolve o dict da marca (com "id" e "label")
    ou None se nao encontrar."""
    texto_norm = normalizar_texto(texto)
    marcas = metricool_listar_marcas()
    melhor = None
    for marca in marcas:
        nome_norm = normalizar_texto(marca.get("label", ""))
        if nome_norm and nome_norm in texto_norm:
            if not melhor or len(nome_norm) > len(normalizar_texto(melhor.get("label", ""))):
                melhor = marca
    return melhor


def _metricool_periodo(dias):
    """Calcula o inicio/fim (hoje, horario de Bahia) de um periodo de N dias pra
    consulta de metricas, devolvendo tanto o formato YYYY-MM-DD (usado pelos
    endpoints /stats/*) quanto o ISO completo (usado pelos endpoints /v2/analytics/*)."""
    fim = horario_bahia_agora()
    inicio = fim - timedelta(days=max(dias, 1))
    return inicio, fim


def metricool_seguidores(blog_id, rede, dias):
    """Evolucao de seguidores no periodo, via /stats/timeling/{metrica} - confirmado
    na documentacao oficial do Metricool só pra Instagram (metrica "igFollowers");
    outras redes ainda nao tem a metrica confirmada, entao avisamos a limitacao em
    vez de arriscar um endpoint que pode nem existir."""
    if rede != "instagram":
        return False, "Por enquanto só consigo consultar evolução de seguidores pro Instagram - Facebook ainda não está disponível."
    inicio, fim = _metricool_periodo(dias)
    try:
        resp = requests.get(
            f"{METRICOOL_BASE_URL}/stats/timeling/igFollowers",
            headers=metricool_headers(),
            params={
                "userId": METRICOOL_USER_ID, "blogId": blog_id,
                "start": inicio.strftime("%Y%m%d"), "end": fim.strftime("%Y%m%d"),
            },
            timeout=20,
        )
        if resp.status_code >= 400:
            print(f"[metricool_seguidores] ERRO {resp.status_code}: {resp.text[:800]}", flush=True)
            return False, f"O Metricool não conseguiu me dar essa informação agora (erro {resp.status_code})."
        corpo = resp.json()
        pontos = corpo.get("data", corpo) if isinstance(corpo, dict) else corpo
        if not pontos:
            return True, f"Não encontrei dados de seguidores do Instagram nos últimos {dias} dias."
        primeiro = pontos[0].get("value", pontos[0].get("y"))
        ultimo = pontos[-1].get("value", pontos[-1].get("y"))
        if primeiro is None or ultimo is None:
            return True, f"Encontrei dados de seguidores, mas não consegui ler os números direito - dá uma conferida direto no Metricool pra garantir."
        diferenca = ultimo - primeiro
        sinal = "+" if diferenca >= 0 else ""
        return True, (
            f"Seguidores no Instagram nos últimos {dias} dias: {int(ultimo)} agora "
            f"({sinal}{int(diferenca)} em relação a {inicio.strftime('%d/%m')})."
        )
    except Exception as e:
        print(f"[metricool_seguidores] falhou: {e}", flush=True)
        return False, "Não consegui falar com o Metricool agora, tenta de novo daqui a pouco?"


def _somar_metricas_posts(lista_posts):
    """Soma os campos numericos mais comuns que os posts do Metricool trazem -
    escrito de forma tolerante a variacao de nome de campo entre versoes da API
    (ex: "shares" vs "shareCount"), ja que nao tenho o schema oficial completo."""
    def pegar(post, *chaves):
        for chave in chaves:
            valor = post.get(chave)
            if isinstance(valor, (int, float)):
                return valor
        return 0

    totais = {"likes": 0, "comentarios": 0, "compartilhamentos": 0, "alcance": 0, "impressoes": 0}
    for post in lista_posts:
        totais["likes"] += pegar(post, "likes", "likeCount")
        totais["comentarios"] += pegar(post, "comments", "commentCount")
        totais["compartilhamentos"] += pegar(post, "shares", "shareCount")
        totais["alcance"] += pegar(post, "reach", "reachCount")
        totais["impressoes"] += pegar(post, "impressions", "impressionCount")
    return totais


def metricool_resumo_posts_ou_reels(blog_id, rede, tipo, dias):
    """Busca posts ou reels publicados no periodo (/v2/analytics/posts|reels/{rede})
    e devolve um resumo agregado em texto. Endpoints confirmados: reels/instagram
    (doc oficial do Metricool); posts/{rede} e reels/facebook seguem o mesmo padrao
    de URL, mas ainda nao foram testados em produção - se a API recusar, avisamos
    o erro real em vez de inventar um numero."""
    inicio, fim = _metricool_periodo(dias)
    caminho = "reels" if tipo == "reels" else "posts"
    try:
        resp = requests.get(
            f"{METRICOOL_BASE_URL}/v2/analytics/{caminho}/{rede}",
            headers=metricool_headers(),
            params={
                "userId": METRICOOL_USER_ID, "blogId": blog_id,
                "from": inicio.strftime("%Y-%m-%dT00:00:00"), "to": fim.strftime("%Y-%m-%dT23:59:59"),
            },
            timeout=20,
        )
        if resp.status_code >= 400:
            print(f"[metricool_resumo_posts_ou_reels] ERRO {resp.status_code}: {resp.text[:800]}", flush=True)
            return False, f"O Metricool não conseguiu me dar essa informação agora (erro {resp.status_code})."
        corpo = resp.json()
        lista = corpo.get("data", corpo) if isinstance(corpo, dict) else corpo
        if not lista:
            return True, f"Não teve nenhum {'reel' if tipo == 'reels' else 'post'} publicado nos últimos {dias} dias."
        totais = _somar_metricas_posts(lista)
        nome_tipo = "reels" if tipo == "reels" else "posts"
        return True, (
            f"{len(lista)} {nome_tipo} nos últimos {dias} dias: {totais['likes']} curtidas, "
            f"{totais['comentarios']} comentários, {totais['compartilhamentos']} compartilhamentos, "
            f"alcance de {totais['alcance']} e {totais['impressoes']} impressões (somados)."
        )
    except Exception as e:
        print(f"[metricool_resumo_posts_ou_reels] falhou: {e}", flush=True)
        return False, "Não consegui falar com o Metricool agora, tenta de novo daqui a pouco?"


def metricool_responder_metricas(marca, rede, tipo, dias):
    """Ponto de entrada unico pra qualquer pergunta de metrica: escolhe a funcao
    certa pelo tipo pedido e devolve o texto pronto pra mandar no WhatsApp."""
    blog_id = marca["id"]
    if tipo == "seguidores":
        sucesso, texto = metricool_seguidores(blog_id, rede, dias)
    else:
        sucesso, texto = metricool_resumo_posts_ou_reels(blog_id, rede, tipo, dias)
    prefixo = f"📊 *{marca['label']}* ({rede}):\n" if sucesso else ""
    return f"{prefixo}{texto}"


_REGEX_REGRA_CLIENTE = re.compile(r"^\s*regra\s+(?:para|pro|pra)\s+(.+?)\s*:\s*(.+)$", re.IGNORECASE | re.DOTALL)


def processar_dm(remote_jid, key, data):
    if numero_bate(remote_jid, TORRES_NUMBER):
        pessoa, numero = "torres", TORRES_NUMBER
    elif numero_bate(remote_jid, LUAN_NUMBER):
        pessoa, numero = "luan", LUAN_NUMBER
    else:
        return {"skipped": "DM de número não reconhecido"}

    # Registra TUDO que Torres/Luan falam ou mandam no privado com a Cintia, pra servir
    # de historico/backup (assim como ja fazemos com os grupos de cliente) - sistema de
    # defesa contra falha/esquecimento, nao so pra lembrete/fato/comando reconhecido.
    grupo_jid_dm = f"dm_{pessoa}"
    grupo_nome_dm = "Privado - Torres" if pessoa == "torres" else "Privado - Luan"

    def responder(texto_resposta):
        """Manda a resposta pro WhatsApp e guarda ela tambem no historico do DM, do
        mesmo jeito que a mensagem recebida - assim a proxima mensagem da pessoa (e
        qualquer consulta de historico) enxerga os dois lados da conversa, nao so o
        que ela escreveu. Sem isso, um "isso"/"está público"/"foi isso mesmo" logo
        depois de uma pergunta da Cintia nao tinha como ser entendido."""
        registrar_mensagem_grupo(grupo_jid_dm, grupo_nome_dm, "Cintia", texto_resposta, False)
        enviar_texto(numero, texto_resposta)

    message = data.get("message", {})
    message_type = data.get("messageType", "")
    tipo_lower = message_type.lower()

    # Foto ou PDF no privado = pedido de revisao de peca (nao de lembrete).
    if "image" in tipo_lower or "document" in tipo_lower:
        registrar_mensagem_grupo(grupo_jid_dm, grupo_nome_dm, pessoa, "[enviou imagem/PDF pra revisão de arte]", True)
        return revisar_arte_dm(numero, key, data, message_type)

    # Audio/PTT no privado: transcreve e trata como se fosse uma mensagem de texto normal
    # dali pra frente (pode ser um pedido de lembrete, uma regra, uma pergunta, etc) - antes
    # isso caia direto no "nao tratado" e nem era registrado no historico.
    if "audio" in tipo_lower or "ptt" in tipo_lower:
        b64_audio = baixar_midia_evolution(key)
        if b64_audio:
            with tempfile.NamedTemporaryFile(suffix=".ogg", delete=False) as f:
                f.write(base64.b64decode(b64_audio))
                caminho_audio = f.name
            texto = transcrever_audio(caminho_audio)
            os.unlink(caminho_audio)
        else:
            texto = ""
        if not texto:
            registrar_mensagem_grupo(grupo_jid_dm, grupo_nome_dm, pessoa, "[mandou um áudio que não pôde ser transcrito]", True)
            responder("Recebi seu áudio mas não consegui entender o que foi dito, pode escrever ou mandar de novo?")
            return {"skipped": "áudio não transcrito"}
        registrar_mensagem_grupo(grupo_jid_dm, grupo_nome_dm, pessoa, f"[áudio] {texto}", True)
    # Video no privado: ainda nao processamos pedido de video (so imagem/PDF), mas registra
    # no historico mesmo assim, em vez de ficar completamente de fora do "sistema de defesa".
    elif "video" in tipo_lower:
        caption_video = message.get("videoMessage", {}).get("caption", "")
        conteudo_video = f"[vídeo com a legenda: {caption_video}]" if caption_video else "[mandou um vídeo, sem legenda]"
        registrar_mensagem_grupo(grupo_jid_dm, grupo_nome_dm, pessoa, conteudo_video, True)
        responder("Recebi o vídeo! Só não processo vídeo diretamente por aqui ainda (só imagem e PDF) - mas já fica registrado.")
        return {"video_registrado": True}
    else:
        texto = message.get("conversation", "")
        if not texto:
            # Qualquer outro tipo de mensagem que a gente ainda nao trata explicitamente
            # (figurinha, localizacao, contato, enquete, etc) - registra ao menos que algo
            # chegou, em vez de sumir completamente do historico.
            registrar_mensagem_grupo(grupo_jid_dm, grupo_nome_dm, pessoa, f"[mandou uma mensagem do tipo '{message_type}', não suportada ainda]", True)
            return {"skipped": "DM de tipo não tratado nesta versão, mas registrado no histórico"}
        registrar_mensagem_grupo(grupo_jid_dm, grupo_nome_dm, pessoa, texto, True)

    # "regra pro/pra/para <cliente>: <instrucao>" - mesma ideia da regra geral, mas
    # vinculada a UM cliente especifico (ex: "regra pro Zurca: quando perguntarem sobre
    # gravacao, pode dizer que e marcada direto comigo") - nunca vaza pra outro cliente.
    m_regra_cliente = _REGEX_REGRA_CLIENTE.match(texto)
    if m_regra_cliente:
        nome_cliente_regra = m_regra_cliente.group(1).strip()
        texto_regra_cliente = m_regra_cliente.group(2).strip()
        grupo_jid_regra = identificar_grupo_mencionado(nome_cliente_regra)
        grupo_regra = GRUPOS.get(grupo_jid_regra) if grupo_jid_regra else None
        if grupo_regra and not grupo_regra.get("interno") and texto_regra_cliente:
            nome_canonico = grupo_regra["nome"]
            salvar_regra(pessoa, texto_regra_cliente, cliente=nome_canonico)
            responder(f'Anotado! ✅ Regra específica pro cliente {nome_canonico}: "{texto_regra_cliente}"')
            return {"regra_cliente_salva": texto_regra_cliente, "cliente": nome_canonico}
        elif not texto_regra_cliente:
            responder("Entendi que é uma regra específica de cliente, mas não veio nenhuma instrução depois dos dois pontos. Pode mandar de novo?")
            return {"skipped": "regra de cliente vazia"}
        else:
            responder(f'Entendi que é uma regra pro cliente "{nome_cliente_regra}", mas não achei esse cliente cadastrado. Pode confirmar o nome certinho?')
            return {"skipped": "cliente nao encontrado pra regra especifica"}

    # Palavra-chave "regra:" tem prioridade maxima sobre qualquer outra logica - e como
    # Torres/Luan ensinam uma instrucao permanente de atendimento, que passa a valer pra
    # toda resposta automatica de cliente dali pra frente (guardada no banco).
    if texto.strip().lower().startswith("regra:"):
        texto_regra = texto.split(":", 1)[1].strip()
        if texto_regra:
            salvar_regra(pessoa, texto_regra)
            responder(f'Anotado! ✅ Vou seguir essa regra a partir de agora: "{texto_regra}"')
            return {"regra_salva": texto_regra, "autor": pessoa}
        else:
            responder("Entendi que é uma regra nova, mas não veio nenhum texto depois de \"regra:\". Pode mandar de novo com a instrução?")
            return {"skipped": "regra vazia"}

    # Pedido de correção de texto ("corrija o texto com o tom formal/cordial: ...") tem
    # prioridade sobre a lógica de lembrete - não é um lembrete, é outra ferramenta.
    modo_correcao, texto_a_corrigir = extrair_modo_e_texto_correcao(texto)
    if modo_correcao:
        return corrigir_texto_dm(numero, modo_correcao, texto_a_corrigir)

    # Se tem um comando "pro Tripa" pendente de confirmacao pra essa pessoa, confere se
    # essa mensagem e um sim/nao curto antes de tratar como mensagem nova.
    pendente = _comandos_pendentes.get(pessoa)
    if pendente and (time.time() - pendente["criado_em"]) <= _COMANDO_PENDENTE_TTL:
        confirma = parece_confirmacao(texto)
        if confirma is True:
            enviar_texto(TRIPA_DESIGNER_JID, pendente["mensagem_tripa"])
            aviso_cobranca = ""
            if pendente.get("tem_cobranca") and pendente.get("horario_cobranca"):
                # Se o horario combinado ja passou entre a previa e a confirmacao (ex: a
                # pessoa demorou pra responder "sim"), avisa que a cobranca vai sair agora em
                # vez de deixar isso acontecer silenciosamente sem explicar o motivo.
                if pendente["horario_cobranca"] <= datetime.now(timezone.utc):
                    aviso_cobranca = " Como o horário combinado já passou, vou perguntar pra Tripa agora mesmo."
                else:
                    aviso_cobranca = " Vou cobrar eles no horário combinado."
                agendar_cobranca_tripa(pendente["horario_cobranca"], pendente["pergunta_cobranca"])
            _comandos_pendentes.pop(pessoa, None)
            responder("Show, encaminhei pra Tripa! ✅" + aviso_cobranca)
            return {"comando_tripa_confirmado": True}
        elif confirma is False:
            _comandos_pendentes.pop(pessoa, None)
            responder("Beleza, não mandei nada. Se quiser, me manda de novo do jeito certo.")
            return {"comando_tripa_cancelado": True}
        # confirma is None: nao pareceu sim/nem nao, segue o fluxo normal (pode ser uma
        # mensagem nova, ou uma correcao ao comando pendente - nesse caso o comando antigo
        # so expira depois do TTL, ou e substituido se essa mensagem virar um novo comando).

    # Se tem uma promessa detectada aguardando confirmacao (Torres OU Luan podem responder -
    # nao e por pessoa como o comando pro Tripa, porque os dois recebem o aviso), confere a
    # mais antiga primeiro. Descarta as que passaram do TTL sem resposta.
    agora_ts = time.time()
    while _promessas_pendentes and (agora_ts - _promessas_pendentes[0]["criado_em"]) > _PROMESSA_PENDENTE_TTL:
        _promessas_pendentes.pop(0)
    if _promessas_pendentes:
        confirma_promessa = parece_confirmacao(texto)
        if confirma_promessa is True:
            promessa = _promessas_pendentes.pop(0)
            salvar_fato(pessoa, promessa["texto"])
            responder(f"Anotado! ✅ Vou guardar: \"{promessa['texto']}\"")
            return {"promessa_guardada": promessa["texto"]}
        elif confirma_promessa is False:
            _promessas_pendentes.pop(0)
            responder("Beleza, não guardei nada.")
            return {"promessa_descartada": True}
        # None: nao pareceu confirmacao, segue o fluxo normal - a promessa continua na fila
        # esperando ate o TTL expirar ou alguem confirmar/recusar.

    # Se já existe lembrete pendente pra essa pessoa, qualquer resposta encerra o nag.
    tinha_pendente = marcar_resolvido(pessoa)

    agora = horario_bahia_agora()
    fatos = listar_fatos()
    contexto_fatos = (
        "FATOS QUE VOCÊ JÁ SABE (use quando fizer sentido pra responder):\n"
        + "\n".join(f"- {f}" for f in fatos) + "\n\n"
    ) if fatos else ""
    # Ultimas mensagens trocadas nesse DM (dos dois lados, ja que agora a Cintia
    # tambem registra o que ela mesma responde) - sem isso, cada mensagem era
    # classificada isolada, sem noção do que tinha acabado de ser perguntado/
    # combinado, e por isso um "sim"/"isso"/"está público" solto não fazia sentido
    # pra ela fora do fluxo de confirmação pendente.
    historico_dm = buscar_mensagens_recentes_grupo(grupo_jid_dm, limite=13)[:-1]
    contexto_conversa = (
        "ÚLTIMAS MENSAGENS DESSA CONVERSA (mais antiga primeiro - use pra entender o\n"
        "contexto, ex: se \"isso\"/\"esse\"/\"sim\" está se referindo a algo que você acabou\n"
        "de perguntar ou a um pedido de poucas mensagens atrás):\n"
        + "\n".join(f"- {m['autor']}: {m['conteudo']}" for m in historico_dm) + "\n\n"
    ) if historico_dm else ""
    lista_grupos = "\n".join(
        f"- {info['nome']}" for info in GRUPOS.values() if not info.get("interno")
    )
    prompt_sistema = (
        SYSTEM_PROMPT_LEMBRETE
        .replace("{agora_iso}", agora.isoformat())
        .replace("{contexto_conversa}", contexto_conversa)
        .replace("{contexto_fatos}", contexto_fatos)
        .replace("{pessoa_nome}", "Torres" if pessoa == "torres" else "Luan")
        .replace("{lista_grupos}", lista_grupos)
    )
    try:
        resultado = chamar_claude(prompt_sistema, texto)
    except Exception as e:
        responder("Tive um problema pra processar sua mensagem agora, pode mandar de novo?")
        return {"erro_claude": str(e), "lembrete_anterior_resolvido": tinha_pendente}

    if resultado.get("eh_pedido_de_lembrete"):
        try:
            alvo = datetime.fromisoformat(resultado["data_hora_alvo_iso"])
        except Exception:
            responder("Entendi que você quer um lembrete, mas não consegui identificar o horário certinho. Pode me falar de novo com a hora?")
            return {"erro": "não conseguiu parsear data_hora_alvo_iso", "resultado": resultado}

        texto_lembrete = resultado.get("texto_lembrete", texto)
        destino = resolver_destinatario_lembrete(resultado.get("destinatario_lembrete"))
        if destino:
            destinatario_key, numero_destino, quem_recebe, repetir = destino
        else:
            destinatario_key, numero_destino, quem_recebe, repetir = pessoa, numero, "você", True

        eh_recorrente = bool(resultado.get("eh_recorrente"))
        dia_mes_str = str(resultado.get("recorrencia_dia_mes") or "").strip()
        if eh_recorrente and dia_mes_str:
            try:
                dia_mes = max(1, min(31, int(dia_mes_str)))
            except Exception:
                dia_mes = alvo.day
            agendar_lembrete_recorrente(destinatario_key, numero_destino, dia_mes, alvo.hour, alvo.minute, texto_lembrete, repetir_ate_confirmar=repetir)
            quando = f"todo dia {dia_mes} de cada mês às {alvo.strftime('%H:%M')}"
        else:
            agendar_lembrete(destinatario_key, numero_destino, alvo, texto_lembrete, repetir_ate_confirmar=repetir)
            quando = f"às {alvo.strftime('%H:%M')}" if not repetir else "10 min antes"

        if quem_recebe == "você":
            responder(f"Combinado! Vou te lembrar {quando}: \"{texto_lembrete}\" 👍")
        else:
            responder(f"Combinado! Vou lembrar {quem_recebe} {quando}: \"{texto_lembrete}\" 👍")
    elif resultado.get("eh_fato_para_lembrar") and resultado.get("fato_texto"):
        salvar_fato(pessoa, resultado["fato_texto"])
        responder(f"Anotado! ✅ Vou lembrar: \"{resultado['fato_texto']}\"")
    elif resultado.get("eh_pergunta_atividade_geral"):
        pessoa_nome_geral = "Torres" if pessoa == "torres" else "Luan"
        responder(responder_atividade_geral_hoje(pessoa_nome_geral))
    elif resultado.get("eh_pergunta_sobre_grupo") and resultado.get("grupo_perguntado"):
        grupo_jid_pergunta = identificar_grupo_mencionado(resultado["grupo_perguntado"])
        if not grupo_jid_pergunta:
            enviar_texto(
                numero,
                f"Entendi que a pergunta é sobre o grupo \"{resultado['grupo_perguntado']}\", mas não achei "
                "esse grupo aqui. Pode confirmar o nome certinho?",
            )
        else:
            grupo_nome_pergunta = GRUPOS[grupo_jid_pergunta]["nome"]
            pessoa_nome = "Torres" if pessoa == "torres" else "Luan"
            resposta = responder_pergunta_sobre_grupo(pessoa_nome, texto, grupo_jid_pergunta, grupo_nome_pergunta)
            responder(resposta)
    elif resultado.get("eh_comando_para_tripa") and resultado.get("mensagem_tripa"):
        mensagem_tripa = resultado["mensagem_tripa"]
        tem_cobranca = bool(resultado.get("tem_cobranca"))
        horario_cobranca = None
        pergunta_cobranca = resultado.get("pergunta_cobranca") or "Como está esse pedido? Já foi feito?"
        preview_cobranca = ""
        if tem_cobranca and resultado.get("horario_cobranca_iso"):
            try:
                horario_cobranca = datetime.fromisoformat(resultado["horario_cobranca_iso"])
                if horario_cobranca <= datetime.now(timezone.utc):
                    # O horario pedido (ex: "9h40") ja passou no momento em que a mensagem foi
                    # mandada/processada - avisa isso ja na previa, em vez de deixar a pessoa
                    # confirmar sem saber que a cobranca vai sair quase na hora, e nao no
                    # horario que ela pediu.
                    preview_cobranca = (
                        f"\n\n⚠️ O horário de cobrança que ficou combinado ({horario_cobranca.strftime('%H:%M')}) "
                        "já passou - se confirmar, eu já pergunto pra Tripa agora mesmo, em vez de esperar."
                    )
                else:
                    preview_cobranca = f"\n\n⏰ Vou cobrar a Tripa às {horario_cobranca.strftime('%H:%M')} perguntando: \"{pergunta_cobranca}\""
            except Exception:
                tem_cobranca = False
        _comandos_pendentes[pessoa] = {
            "mensagem_tripa": mensagem_tripa,
            "tem_cobranca": tem_cobranca,
            "horario_cobranca": horario_cobranca,
            "pergunta_cobranca": pergunta_cobranca,
            "criado_em": time.time(),
        }
        enviar_texto(
            numero,
            f"Ficou assim pra encaminhar pra Tripa:\n\n{mensagem_tripa}{preview_cobranca}\n\n"
            "Confirma que posso mandar? (responde \"sim\" ou \"não\")",
        )
    elif resultado.get("eh_comando_briefing_cliente") and resultado.get("briefing_cliente_nome"):
        pessoa_nome_briefing = "Torres" if pessoa == "torres" else "Luan"
        nome_mencionado = resultado["briefing_cliente_nome"]
        candidatos = identificar_grupos_candidatos(nome_mencionado)
        if not candidatos:
            responder(
                f"Entendi que é pra analisar a conversa do cliente \"{nome_mencionado}\", mas não "
                "achei esse cliente cadastrado. Pode confirmar o nome certinho?"
            )
        elif len(candidatos) > 1:
            # Nunca escolhe sozinho quando mais de um cliente/grupo bate com a mesma mencao -
            # pergunta qual e o certo, em vez de arriscar montar o briefing do cliente errado.
            nomes_candidatos = ", ".join(GRUPOS[jid]["nome"] for jid in candidatos)
            responder(
                f"Encontrei mais de um cliente relacionado a \"{nome_mencionado}\": {nomes_candidatos}. "
                "Qual deles você quer que eu analise?"
            )
        else:
            grupo_jid_briefing = candidatos[0]
            grupo_nome_briefing = GRUPOS[grupo_jid_briefing]["nome"]
            briefing = montar_briefing_cliente(
                pessoa_nome_briefing, grupo_jid_briefing, grupo_nome_briefing,
                resultado.get("briefing_assunto") or "",
            )
            if briefing.get("duvida_ambigua"):
                pergunta = briefing.get("pergunta_duvida") or (
                    f"Analisei a conversa do {grupo_nome_briefing}, mas encontrei algo que não "
                    "consegui confirmar com segurança antes de montar o briefing. Pode me ajudar?"
                )
                responder(pergunta)
            elif not briefing.get("pedidos"):
                # Nenhum pedido identificado e o modelo nao marcou duvida_ambigua explicitamente -
                # nunca manda algo vazio/inventado pro Tripa, so avisa que nao achou nada claro.
                responder(
                    f"Analisei a conversa do {grupo_nome_briefing}, mas não consegui identificar "
                    "nenhuma solicitação clara pra encaminhar. Pode confirmar o que você quer que "
                    "eu procure?"
                )
            else:
                pedidos = briefing["pedidos"]
                # Uma conversa pode conter mais de um pedido distinto (ex: a arte de uma promoção
                # E, separadamente, incluir um artista na programação) - cada um vira sua propria
                # secao no briefing, nunca misturados; com um pedido so, mantem o formato simples
                # ja usado no resto do sistema (CLIENTE/SOLICITAÇÃO/DESCRIÇÃO).
                if len(pedidos) == 1:
                    p = pedidos[0]
                    mensagem_tripa_briefing = (
                        f"*CLIENTE:* {grupo_nome_briefing}\n"
                        f"*SOLICITAÇÃO:* {p.get('tipo_peca') or 'Arte'}\n"
                        f"*DESCRIÇÃO:* {p.get('descricao_briefing') or ''}\n\n"
                        "_Já considera as alterações feitas durante a conversa com o cliente._"
                    )
                else:
                    secoes = []
                    for i, p in enumerate(pedidos, start=1):
                        secoes.append(
                            f"{i}. *{p.get('titulo') or f'Pedido {i}'}*"
                            + (f" ({p.get('tipo_peca')})" if p.get("tipo_peca") else "")
                            + f"\n{p.get('descricao_briefing') or ''}"
                        )
                    mensagem_tripa_briefing = (
                        f"*CLIENTE:* {grupo_nome_briefing}\n"
                        f"Temos {len(pedidos)} solicitações confirmadas na conversa:\n\n"
                        + "\n\n".join(secoes)
                        + "\n\n_Já considera as alterações feitas durante a conversa com o cliente._"
                    )
                _comandos_pendentes[pessoa] = {
                    "mensagem_tripa": mensagem_tripa_briefing,
                    "tem_cobranca": False,
                    "horario_cobranca": None,
                    "pergunta_cobranca": "",
                    "criado_em": time.time(),
                }
                resumo_curto = briefing.get("resumo_curto") or f"Briefing do {grupo_nome_briefing} pronto."
                responder(
                    f"Analisei a conversa do {grupo_nome_briefing}! {resumo_curto}\n\n"
                    f"Ficou assim pra encaminhar pra Tripa:\n\n{mensagem_tripa_briefing}\n\n"
                    "Confirma que posso mandar? (responde \"sim\" ou \"não\")"
                )
    elif resultado.get("eh_pergunta_metricool_metricas"):
        nome_cliente_metrica = resultado.get("metricool_metrica_cliente") or ""
        marca_metrica = metricool_identificar_marca(nome_cliente_metrica)
        rede_metrica = resultado.get("metricool_metrica_rede") or "instagram"
        tipo_metrica = resultado.get("metricool_metrica_tipo") or "posts"
        dias_metrica = resultado.get("metricool_metrica_dias") or 30
        if not marca_metrica:
            enviar_texto(
                numero,
                f"Entendi que é sobre métricas do cliente \"{nome_cliente_metrica}\", mas não achei "
                "essa marca cadastrada no Metricool. Pode confirmar o nome certinho?",
            )
        else:
            responder(metricool_responder_metricas(marca_metrica, rede_metrica, tipo_metrica, dias_metrica))
    elif tinha_pendente:
        responder("Combinado, marquei como resolvido! ✅")
    else:
        # Nao e pedido de lembrete nem resposta fechando uma pendencia - mesmo assim,
        # toda mensagem no privado precisa de alguma resposta (nunca ficar em silencio).
        resposta_conversa = resultado.get("resposta_conversa") or "Beleza, recebi aqui! 👍"
        responder(resposta_conversa)

    return {"resultado": resultado, "lembrete_anterior_resolvido": tinha_pendente}


# --------------------------------------------------------------------------
# Rota principal do webhook
# --------------------------------------------------------------------------

@app.route("/webhook", methods=["POST"])
def webhook():
    body = request.get_json(silent=True) or {}
    print(f"[webhook] payload bruto: {json.dumps(body)[:3000]}", flush=True)
    data = body.get("data", body)

    key = data.get("key", {})
    remote_jid = key.get("remoteJid", "")
    msg_id = key.get("id", "")
    from_me = key.get("fromMe", False)

    print(f"[webhook] remote_jid={remote_jid} msg_id={msg_id} from_me={from_me} messageType={data.get('messageType')}", flush=True)

    if not remote_jid or from_me:
        return jsonify({"ok": True, "skipped": "sem remoteJid ou mensagem própria"})

    if not msg_id or already_processed(msg_id):
        return jsonify({"ok": True, "skipped": "duplicado"})

    try:
        if remote_jid == TRIPA_DESIGNER_JID:
            print("[webhook] grupo Tripa Designer - checando se tem peça pra revisar", flush=True)
            resultado = processar_revisao_grupo_designer(remote_jid, key, data)
        elif remote_jid in GRUPOS:
            grupo = GRUPOS[remote_jid]
            print(f"[webhook] grupo reconhecido: {grupo['nome']} (interno={grupo['interno']})", flush=True)
            if grupo["interno"]:
                # Grupo interno (ex: "Correria - Gestão") nunca recebe auto-resposta, mas o
                # historico continua sendo registrado igual a qualquer outro grupo - Torres
                # pediu explicitamente que TODOS os grupos e todos os participantes (ele e o
                # Luan incluidos) fiquem guardados, sem excecao pros grupos internos.
                conteudo_log, _im, _pdf, _doc = extrair_conteudo_mensagem_grupo(key, data)
                if conteudo_log:
                    _participant_interno, eh_equipe_interno = _detectar_participante_grupo(key, data)
                    sender_name_interno = data.get("pushName", "equipe")
                    registrar_mensagem_grupo(remote_jid, grupo["nome"], sender_name_interno, conteudo_log, eh_equipe_interno)
                return jsonify({"ok": True, "skipped": "grupo interno, sem auto-resposta"})
            resultado = processar_mensagem_grupo(remote_jid, grupo, key, data)
        elif remote_jid.endswith("@g.us"):
            print(f"[webhook] grupo NÃO reconhecido (não está no dicionário GRUPOS): {remote_jid} - registrando mesmo assim", flush=True)
            resultado = processar_mensagem_grupo_desconhecido(remote_jid, key, data)
        elif remote_jid.endswith("@s.whatsapp.net") or remote_jid.endswith("@lid"):
            print(f"[webhook] tratando como DM: {remote_jid}", flush=True)
            resultado = processar_dm(remote_jid, key, data)
        else:
            print(f"[webhook] origem não tratada: {remote_jid}", flush=True)
            return jsonify({"ok": True, "skipped": "origem não tratada"})
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"ok": False, "erro": str(e)}), 200

    print(f"[webhook] resultado final: {resultado}", flush=True)
    return jsonify({"ok": True, "resultado": resultado})


@app.route("/", methods=["GET"])
def health():
    return jsonify({"status": "ok", "service": "kingkong-whatsapp-webhook"})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, threaded=True)
