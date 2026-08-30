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
                    criado_em TIMESTAMPTZ NOT NULL DEFAULT now()
                )
            """)
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


def salvar_regra(autor, texto):
    if DATABASE_URL:
        try:
            with db_cursor(commit=True) as cur:
                cur.execute(
                    "INSERT INTO regras_atendimento (autor, texto) VALUES (%s, %s)",
                    (autor, texto),
                )
            return
        except Exception as e:
            print(f"[salvar_regra] banco de dados falhou, usando fallback em memoria: {e}", flush=True)
    with _regras_lock:
        _regras_memoria.append({"autor": autor, "texto": texto})


def listar_regras():
    if DATABASE_URL:
        try:
            with db_cursor() as cur:
                cur.execute("SELECT texto FROM regras_atendimento ORDER BY criado_em ASC")
                return [r["texto"] for r in cur.fetchall()]
        except Exception as e:
            print(f"[listar_regras] erro: {e}", flush=True)
            return []
    with _regras_lock:
        return [r["texto"] for r in _regras_memoria]


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
    perguntar sobre um grupo pelo nome sem precisar citar o JID."""
    texto_norm = normalizar_texto(texto)
    melhor = None
    for jid, info in GRUPOS.items():
        if info.get("interno"):
            continue
        nome_norm = normalizar_texto(info["nome"])
        if nome_norm and nome_norm in texto_norm:
            if not melhor or len(nome_norm) > len(normalizar_texto(GRUPOS[melhor]["nome"])):
                melhor = jid
    return melhor


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
    """Lista tarefas com status != CONCLUIDO, opcionalmente filtrando por cliente. Ainda
    nao usado em nenhum fluxo automatico - preparado pra futura cobranca de pendencias no
    fim do expediente."""
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


init_db()  # roda uma vez quando o servico sobe (seja via gunicorn ou python app.py)


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


def horario_bahia_agora() -> datetime:
    return datetime.now(timezone.utc) - timedelta(hours=3)


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
            "model": "claude-haiku-4-5",
            "max_tokens": 800,
            "system": system_prompt,
            "messages": [{"role": "user", "content": messages_content}],
        },
        timeout=30,
    )
    if resp.status_code >= 400:
        print(f"[chamar_claude] ERRO {resp.status_code}: {resp.text[:1000]}", flush=True)
        raise RuntimeError(f"Claude API {resp.status_code}: {resp.text[:500]}")
    texto = resp.json()["content"][0]["text"].strip()
    # As vezes o modelo embrulha o JSON num bloco de codigo markdown - remove isso.
    if texto.startswith("```"):
        texto = re.sub(r"^```(?:json)?\s*", "", texto)
        texto = re.sub(r"\s*```$", "", texto)
    try:
        return json.loads(texto)
    except json.JSONDecodeError:
        print(f"[chamar_claude] resposta nao-JSON do Claude: {texto[:500]}", flush=True)
        raise


# --------------------------------------------------------------------------
# Parte 1: resposta automática nos grupos de cliente
# --------------------------------------------------------------------------

SYSTEM_PROMPT_ATENDIMENTO = """Você redige mensagens de WhatsApp em nome da Correria, uma agência de
marketing digital, respondendo clientes que mandam pedidos ou dúvidas em grupos de WhatsApp.
A agência atende três tipos de demanda: pedidos de arte (peças gráficas), pedidos de gravação
(vídeos/filmagens) e dúvidas gerais (status, prazos, etc). A empresa só presta atendimento,
nunca tenta vender nada na resposta.

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


def processar_mensagem_grupo(remote_jid, grupo, key, data):
    # Quem mandou a mensagem DENTRO do grupo (diferente do remote_jid, que e o
    # ID do grupo). Se for a propria equipe (Torres ou Luan) falando no grupo,
    # o robo nunca deve responder nem entrar na conversa.
    # Confere tanto "participant" quanto variantes alternativas que o Whatsapp/Evolution
    # as vezes manda (ex: quando o remetente usa um identificador "lid" em vez do numero
    # de telefone direto) - assim a checagem de "e a propria equipe falando" nao falha so
    # porque o formato do identificador mudou.
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

    sender_name = data.get("pushName", "cliente")
    conteudo_texto, imagem_base64, pdf_base64, nome_arquivo_doc = extrair_conteudo_mensagem_grupo(key, data)

    # Registra a mensagem no historico do grupo (equipe ou cliente) pra Torres/Luan
    # poderem perguntar no privado depois "o que rolou no grupo tal" sem precisar abrir o
    # grupo - guardado independente do que acontece com a auto-resposta.
    if conteudo_texto:
        registrar_mensagem_grupo(remote_jid, grupo["nome"], sender_name, conteudo_texto, eh_equipe)

    if eh_equipe:
        print(f"[processar_mensagem_grupo] mensagem da propria equipe/outro agente ({candidatos_participant}), ignorando", flush=True)
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

    dentro_horario = dentro_do_horario_comercial()
    regras_extras = listar_regras()
    bloco_regras = ""
    if regras_extras:
        bloco_regras = (
            "REGRAS ADICIONAIS QUE A EQUIPE DEFINIU (seguir sempre, têm prioridade sobre "
            "qualquer outra instrução se houver conflito):\n"
            + "\n".join(f"- {r}" for r in regras_extras) + "\n\n"
        )
    prompt_usuario = (
        f"{bloco_regras}"
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

    print(f"[_finalizar_processamento_grupo] concluido pra {remote_jid}: {resultado}", flush=True)


# --------------------------------------------------------------------------
# Parte 2: lembretes pessoais (Torres / Luan)
# --------------------------------------------------------------------------

SYSTEM_PROMPT_LEMBRETE = """Você é a Cintia, assistente virtual da Correria, falando em português num
DM de WhatsApp com {pessoa_nome}, sócio/responsável da agência. A data/hora atual é: {agora_iso}
(horário de Brasília, America/Bahia).

{contexto_fatos}Classifique a mensagem em UM dos tipos abaixo (o mais específico que se aplicar -
um pedido de lembrete não é um comando pro Tripa, um fato pra guardar não é um lembrete, etc):

1) PEDIDO DE NOVO LEMBRETE (ex: "me lembra de ligar pro cliente X às 15h", "lembra eu de mandar o
   orçamento amanhã de manhã") - algo que {pessoa_nome} mesmo(a) quer ser lembrado(a) de fazer.

2) FATO PRA GUARDAR NA MEMÓRIA (ex: "o Luan gosta da cor rosa", "o cliente Terapia prefere painel
   roxo", "meu aniversário é dia X") - uma informação/preferência que não é uma tarefa nem um
   pedido de ação, só algo que deve ficar guardado pra ser usado depois quando fizer sentido
   (inclusive pra responder perguntas futuras tipo "do que o Luan gosta?").

3) PERGUNTA SOBRE O QUE ACONTECEU EM ALGUM GRUPO DE CLIENTE (ex: "o que rolou no grupo do Terapia
   hoje?", "tem pedido pendente lá na Chicafé?", "o cliente Zurca já respondeu?") - {pessoa_nome}
   quer SABER/CONSULTAR algo sobre uma conversa de um grupo específico, SEM pedir nenhuma ação nova
   (isso é diferente do tipo 4: se a mensagem pede pra REPASSAR/ENCAMINHAR algo pro Tripa, mesmo que
   cite o nome de um cliente, é tipo 4, não tipo 3). Preencha "grupo_perguntado" com o nome do grupo
   mencionado, o mais parecido possível com um destes grupos de cliente conhecidos:
   {lista_grupos}

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

5) QUALQUER OUTRA COISA (comentário, resposta a um lembrete anterior, pedido/comando que não se
   encaixa nos tipos acima) - preencha "resposta_conversa" com uma resposta natural e útil, como
   uma colega de equipe responderia no privado. Se os FATOS QUE VOCÊ JÁ SABE (se houver, no topo
   deste prompt) tiverem a resposta pra uma pergunta, use-os pra responder direto. Se for um
   pedido/comando que você ainda não tem como executar automaticamente, confirme que entendeu e que
   vai anotar/repassar, sem inventar que já fez algo que não fez. Nunca deixe esse campo vazio
   quando nenhum dos tipos 1/2/3/4 acima se aplicar - toda mensagem privada precisa de resposta.

IMPORTANTE sobre datas/horários: qualquer campo "*_iso" deve conter APENAS a data/hora em formato
ISO 8601 com fuso -03:00 (exemplo: 2026-08-29T15:00:00-03:00), sem nenhum texto explicativo junto,
interpretando horários relativos ao "agora" informado acima.

Responda SEMPRE E APENAS em JSON válido, numa única linha por valor, neste formato exato,
sem usar bloco de código markdown (nada de ```) e sem quebras de linha dentro dos valores. Inclua
TODAS as chaves sempre, mesmo vazias/false quando não se aplicarem:
{"eh_pedido_de_lembrete": true ou false, "data_hora_alvo_iso": "2026-08-29T15:00:00-03:00", "texto_lembrete": "um resumo curto e claro do que a pessoa quer ser lembrada de fazer", "eh_fato_para_lembrar": true ou false, "fato_texto": "o fato reescrito de forma clara e objetiva, ou string vazia", "eh_pergunta_sobre_grupo": true ou false, "grupo_perguntado": "nome do grupo mencionado, ou string vazia", "eh_comando_para_tripa": true ou false, "mensagem_tripa": "texto pronto pra encaminhar pro grupo Tripa, ou string vazia", "tem_cobranca": true ou false, "horario_cobranca_iso": "horario ISO da cobranca, ou string vazia", "pergunta_cobranca": "pergunta curta pra mandar na cobranca, ou string vazia", "resposta_conversa": "resposta natural pra mensagem, preenchida sempre que nenhum dos tipos 1/2/3/4 acima for verdadeiro"}
"""


# guarda no máximo 1 lembrete ativo por pessoa: {"torres": {...}, "luan": {...}}
lembretes_ativos = {}


def enviar_lembrete(pessoa, numero, texto_lembrete, primeira_vez):
    prefixo = "⏰ Lembrete!" if primeira_vez else "⏰ Lembrete (ainda pendente):"
    enviar_texto(numero, f"{prefixo} {texto_lembrete}")


def agendar_nag(pessoa, numero, texto_lembrete):
    def checar_e_reenviar():
        info = lembretes_ativos.get(pessoa)
        if not info or info.get("resolvido"):
            return
        enviar_lembrete(pessoa, numero, texto_lembrete, primeira_vez=False)

    job = scheduler.add_job(checar_e_reenviar, "interval", minutes=30, id=f"nag-{pessoa}", replace_existing=True)
    return job


def agendar_lembrete(pessoa, numero, data_hora_alvo: datetime, texto_lembrete: str):
    aviso_em = data_hora_alvo - timedelta(minutes=10)
    agora_utc = datetime.now(timezone.utc)
    if aviso_em <= agora_utc:
        aviso_em = agora_utc + timedelta(seconds=5)

    lembretes_ativos[pessoa] = {
        "texto": texto_lembrete,
        "alvo": data_hora_alvo.isoformat(),
        "resolvido": False,
    }

    def disparar_primeiro_aviso():
        info = lembretes_ativos.get(pessoa)
        if not info or info.get("resolvido"):
            return
        enviar_lembrete(pessoa, numero, texto_lembrete, primeira_vez=True)
        agendar_nag(pessoa, numero, texto_lembrete)

    scheduler.add_job(disparar_primeiro_aviso, "date", run_date=aviso_em, id=f"lembrete-{pessoa}-{int(time.time())}")


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
        fila.append({"cliente_nome": cliente_nome, "pedido_texto": pedido_texto, "timestamp": time.time()})


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


SYSTEM_PROMPT_COMPARACAO_PEDIDO = """Você compara uma peça gráfica finalizada com o pedido original
que o cliente fez, pra conferir se a arte contempla todas as informações pedidas.

Você vai receber (1) o pedido original organizado (evento, data, horário, textos, nomes etc que o
cliente pediu) e (2) a imagem ou PDF da arte finalizada. Leia todo o texto visível na arte e
compare com cada item do pedido original.

Aponte:
- Informação que estava no pedido e NÃO aparece na arte (ex: faltou o horário, faltou um nome).
- Informação que aparece na arte mas está DIFERENTE do que foi pedido (ex: data errada, nome
  escrito diferente do pedido, dia da semana que não bate com a data).
Não aponte diferença de design, cores, layout ou estética - só conteúdo/informação. Se a arte
contempla tudo que foi pedido corretamente, diga isso claramente.

Responda SEMPRE E APENAS em JSON válido, sem bloco de código markdown (nada de ```):
{
  "bate_com_pedido": true ou false,
  "problemas": ["lista de itens faltando ou diferentes do pedido, cada um curto e claro"],
  "resumo": "1 frase confirmando que bateu tudo, ou resumindo o principal problema"
}
"""


def comparar_arte_com_pedido(pedido_texto, imagem_base64, pdf_base64):
    prompt_usuario = f"Pedido original do cliente:\n{pedido_texto}\n\nCompare esse pedido com a arte anexada."
    resultado = chamar_claude(SYSTEM_PROMPT_COMPARACAO_PEDIDO, prompt_usuario, imagem_base64=imagem_base64, pdf_base64=pdf_base64)

    bate = bool(resultado.get("bate_com_pedido"))
    if bate:
        texto_resp = f"✅ Conferi com o pedido do cliente: {resultado.get('resumo') or 'bateu tudo certo, contempla o que foi pedido.'}"
    else:
        problemas = resultado.get("problemas") or []
        texto_resp = "⚠️ Comparei com o pedido do cliente e encontrei diferença(s):\n" + "\n".join(f"- {p}" for p in problemas)
        if resultado.get("resumo"):
            texto_resp += f"\n\n{resultado['resumo']}"

    return bate, texto_resp, resultado


def processar_revisao_grupo_designer(remote_jid, key, data):
    """No grupo Tripa Designer: se alguem postar uma foto/PDF de peca, revisa a ortografia
    e SEMPRE avisa no grupo o resultado (bateu ou nao bateu). Se a legenda citar o nome de
    um cliente (ex: "terapia") e tiver um pedido de arte pendente daquele cliente, TAMBEM
    compara a arte com o pedido original e avisa o resultado dessa comparação também."""
    message_type = data.get("messageType", "")
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
            try:
                bate, texto_comparacao, resultado_comparacao = comparar_arte_com_pedido(
                    pedido["pedido_texto"], imagem_base64, pdf_base64
                )
            except Exception as e:
                print(f"[processar_revisao_grupo_designer] erro na comparacao com pedido: {e}", flush=True)
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


SYSTEM_PROMPT_CORRECAO_TEXTO = """Você ajuda a revisar e reescrever, em português do Brasil, um texto
que a pessoa escreveu com dificuldade (pode ter erro de ortografia, gramática, concordância, ou
frases desorganizadas/informais demais). A pessoa pediu o tom "{tom}" pro resultado.

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

_REGEX_PEDIDO_CORRECAO = re.compile(r"^\s*corri[gj]\w*\s+(?:o\s+)?texto\b.*?:", re.IGNORECASE | re.DOTALL)


def extrair_tom_e_texto_correcao(texto_completo):
    """Se a mensagem começar com o padrão 'corrija o texto com o tom formal/cordial: <texto>',
    devolve (tom, texto_a_corrigir). Se não bater com o padrão, devolve (None, None)."""
    m = _REGEX_PEDIDO_CORRECAO.match(texto_completo)
    if not m:
        return None, None
    prefixo = m.group(0).lower()
    tom = "formal" if "formal" in prefixo else "cordial"
    texto_a_corrigir = texto_completo[m.end():].strip()
    return tom, texto_a_corrigir


def corrigir_texto_dm(numero, tom, texto_a_corrigir):
    if not texto_a_corrigir:
        enviar_texto(
            numero,
            "Manda o texto depois dos dois pontos, tipo: \"Corrija o texto com o tom formal: "
            "<seu texto aqui>\"",
        )
        return {"skipped": "pedido de correcao sem texto"}

    prompt_sistema = SYSTEM_PROMPT_CORRECAO_TEXTO.replace("{tom}", tom)
    try:
        resultado = chamar_claude(prompt_sistema, texto_a_corrigir)
    except Exception as e:
        enviar_texto(numero, "Tive um problema pra revisar esse texto agora, pode tentar de novo?")
        return {"erro_claude": str(e)}

    opcao_1 = resultado.get("opcao_1", "")
    opcao_2 = resultado.get("opcao_2", "")
    resposta = f"Aqui vão 2 opções no tom {tom}:\n\n1️⃣ {opcao_1}\n\n2️⃣ {opcao_2}"
    enviar_texto(numero, resposta)
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


def processar_dm(remote_jid, key, data):
    if numero_bate(remote_jid, TORRES_NUMBER):
        pessoa, numero = "torres", TORRES_NUMBER
    elif numero_bate(remote_jid, LUAN_NUMBER):
        pessoa, numero = "luan", LUAN_NUMBER
    else:
        return {"skipped": "DM de número não reconhecido"}

    message = data.get("message", {})
    message_type = data.get("messageType", "")

    # Foto ou PDF no privado = pedido de revisao de peca (nao de lembrete).
    if "image" in message_type.lower() or "document" in message_type.lower():
        return revisar_arte_dm(numero, key, data, message_type)

    texto = message.get("conversation", "")
    if not texto:
        return {"skipped": "DM sem texto (tipo de mensagem não tratado nesta versão)"}

    # Palavra-chave "regra:" tem prioridade maxima sobre qualquer outra logica - e como
    # Torres/Luan ensinam uma instrucao permanente de atendimento, que passa a valer pra
    # toda resposta automatica de cliente dali pra frente (guardada no banco).
    if texto.strip().lower().startswith("regra:"):
        texto_regra = texto.split(":", 1)[1].strip()
        if texto_regra:
            salvar_regra(pessoa, texto_regra)
            enviar_texto(numero, f'Anotado! ✅ Vou seguir essa regra a partir de agora: "{texto_regra}"')
            return {"regra_salva": texto_regra, "autor": pessoa}
        else:
            enviar_texto(numero, "Entendi que é uma regra nova, mas não veio nenhum texto depois de \"regra:\". Pode mandar de novo com a instrução?")
            return {"skipped": "regra vazia"}

    # Pedido de correção de texto ("corrija o texto com o tom formal/cordial: ...") tem
    # prioridade sobre a lógica de lembrete - não é um lembrete, é outra ferramenta.
    tom, texto_a_corrigir = extrair_tom_e_texto_correcao(texto)
    if tom:
        return corrigir_texto_dm(numero, tom, texto_a_corrigir)

    # Se tem um comando "pro Tripa" pendente de confirmacao pra essa pessoa, confere se
    # essa mensagem e um sim/nao curto antes de tratar como mensagem nova.
    pendente = _comandos_pendentes.get(pessoa)
    if pendente and (time.time() - pendente["criado_em"]) <= _COMANDO_PENDENTE_TTL:
        confirma = parece_confirmacao(texto)
        if confirma is True:
            enviar_texto(TRIPA_DESIGNER_JID, pendente["mensagem_tripa"])
            if pendente.get("tem_cobranca") and pendente.get("horario_cobranca"):
                agendar_cobranca_tripa(pendente["horario_cobranca"], pendente["pergunta_cobranca"])
            _comandos_pendentes.pop(pessoa, None)
            enviar_texto(numero, "Show, encaminhei pra Tripa! ✅" + (" Vou cobrar eles no horário combinado." if pendente.get("tem_cobranca") else ""))
            return {"comando_tripa_confirmado": True}
        elif confirma is False:
            _comandos_pendentes.pop(pessoa, None)
            enviar_texto(numero, "Beleza, não mandei nada. Se quiser, me manda de novo do jeito certo.")
            return {"comando_tripa_cancelado": True}
        # confirma is None: nao pareceu sim/nem nao, segue o fluxo normal (pode ser uma
        # mensagem nova, ou uma correcao ao comando pendente - nesse caso o comando antigo
        # so expira depois do TTL, ou e substituido se essa mensagem virar um novo comando).

    # Se já existe lembrete pendente pra essa pessoa, qualquer resposta encerra o nag.
    tinha_pendente = marcar_resolvido(pessoa)

    agora = horario_bahia_agora()
    fatos = listar_fatos()
    contexto_fatos = (
        "FATOS QUE VOCÊ JÁ SABE (use quando fizer sentido pra responder):\n"
        + "\n".join(f"- {f}" for f in fatos) + "\n\n"
    ) if fatos else ""
    lista_grupos = "\n".join(
        f"- {info['nome']}" for info in GRUPOS.values() if not info.get("interno")
    )
    prompt_sistema = (
        SYSTEM_PROMPT_LEMBRETE
        .replace("{agora_iso}", agora.isoformat())
        .replace("{contexto_fatos}", contexto_fatos)
        .replace("{pessoa_nome}", "Torres" if pessoa == "torres" else "Luan")
        .replace("{lista_grupos}", lista_grupos)
    )
    try:
        resultado = chamar_claude(prompt_sistema, texto)
    except Exception as e:
        enviar_texto(numero, "Tive um problema pra processar sua mensagem agora, pode mandar de novo?")
        return {"erro_claude": str(e), "lembrete_anterior_resolvido": tinha_pendente}

    if resultado.get("eh_pedido_de_lembrete"):
        try:
            alvo = datetime.fromisoformat(resultado["data_hora_alvo_iso"])
        except Exception:
            enviar_texto(numero, "Entendi que você quer um lembrete, mas não consegui identificar o horário certinho. Pode me falar de novo com a hora?")
            return {"erro": "não conseguiu parsear data_hora_alvo_iso", "resultado": resultado}
        agendar_lembrete(pessoa, numero, alvo, resultado.get("texto_lembrete", texto))
        enviar_texto(numero, f"Combinado! Vou te lembrar 10 min antes: \"{resultado.get('texto_lembrete', texto)}\" 👍")
    elif resultado.get("eh_fato_para_lembrar") and resultado.get("fato_texto"):
        salvar_fato(pessoa, resultado["fato_texto"])
        enviar_texto(numero, f"Anotado! ✅ Vou lembrar: \"{resultado['fato_texto']}\"")
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
            enviar_texto(numero, resposta)
    elif resultado.get("eh_comando_para_tripa") and resultado.get("mensagem_tripa"):
        mensagem_tripa = resultado["mensagem_tripa"]
        tem_cobranca = bool(resultado.get("tem_cobranca"))
        horario_cobranca = None
        pergunta_cobranca = resultado.get("pergunta_cobranca") or "Como está esse pedido? Já foi feito?"
        preview_cobranca = ""
        if tem_cobranca and resultado.get("horario_cobranca_iso"):
            try:
                horario_cobranca = datetime.fromisoformat(resultado["horario_cobranca_iso"])
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
    elif tinha_pendente:
        enviar_texto(numero, "Combinado, marquei como resolvido! ✅")
    else:
        # Nao e pedido de lembrete nem resposta fechando uma pendencia - mesmo assim,
        # toda mensagem no privado precisa de alguma resposta (nunca ficar em silencio).
        resposta_conversa = resultado.get("resposta_conversa") or "Beleza, recebi aqui! 👍"
        enviar_texto(numero, resposta_conversa)

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
                return jsonify({"ok": True, "skipped": "grupo interno, sem auto-resposta"})
            resultado = processar_mensagem_grupo(remote_jid, grupo, key, data)
        elif remote_jid.endswith("@g.us"):
            print(f"[webhook] grupo NÃO reconhecido (não está no dicionário GRUPOS): {remote_jid}", flush=True)
            return jsonify({"ok": True, "skipped": "grupo não cadastrado"})
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
