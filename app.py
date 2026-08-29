""
Webhook instantâneo - KingKong Filmes
 
Duas funções nesse serviço:
 
1) GRUPOS DE CLIENTE: quando chega mensagem nova num grupo de cliente,
   usa a API da Anthropic (Claude) pra entender a mensagem (texto, imagem
   ou áudio transcrito), responde o cliente na hora no tom combinado, e
   se parecer um cliente chateado/urgente, avisa a equipe (Torres e Luan)
   na hora.
 
2) LEMBRETES PESSOAIS: quando Torres ou Luan mandam uma mensagem direta
   (DM) pro número do robô pedindo um lembrete ("me lembra de ligar pro
   cliente X às 15h"), agenda um aviso 10 minutos antes, e insiste a cada
   30 minutos até a pessoa responder qualquer coisa naquele DM.
 
Variáveis de ambiente necessárias (configurar no Railway, aba Variables
deste serviço):
  ANTHROPIC_API_KEY      - chave da API da Anthropic (console.anthropic.com)
  OPENAI_API_KEY         - chave da API da OpenAI, só usada pra transcrever áudio
  EVOLUTION_BASE_URL     - ex: https://evolution-api-production-d337.up.railway.app
  EVOLUTION_APIKEY       - a AUTHENTICATION_API_KEY da Evolution API
  EVOLUTION_INSTANCE     - nome da instância, ex: Kingkongpedidos
  TORRES_NUMBER          - ex: 5571999394216
  LUAN_NUMBER            - ex: 5571992200583
 
Observação de arquitetura: este serviço guarda lembretes pendentes em
memória (não em banco de dados) - se o serviço reiniciar/redeployar com um
lembrete pendente, ele se perde. Pra esse uso (lembretes do dia a dia,
resolvidos em horas) isso é aceitável; se no futuro isso incomodar, dá pra
evoluir pra guardar isso no Postgres que já roda no mesmo projeto Railway.
Rode este serviço com UMA única instância (não escale horizontalmente) -
o agendador roda na memória do processo.
"""
 
import os
import re
import json
import base64
import tempfile
import time
import threading
from datetime import datetime, timezone, timedelta
 
from flask import Flask, request, jsonify
import requests
from apscheduler.schedulers.background import BackgroundScheduler
 
app = Flask(__name__)
scheduler = BackgroundScheduler(timezone="UTC")
scheduler.start()
 
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
EVOLUTION_BASE_URL = os.environ.get("EVOLUTION_BASE_URL", "").rstrip("/")
EVOLUTION_APIKEY = os.environ.get("EVOLUTION_APIKEY", "")
EVOLUTION_INSTANCE = os.environ.get("EVOLUTION_INSTANCE", "")
TORRES_NUMBER = os.environ.get("TORRES_NUMBER", "5571999394216")
LUAN_NUMBER = os.environ.get("LUAN_NUMBER", "5571992200583")
TEAM_NUMBERS = [TORRES_NUMBER, LUAN_NUMBER]
 
# Grupos monitorados: id -> {"nome": ..., "interno": True/False}
GRUPOS = {
    "120363409281934368@g.us": {"nome": "Terapia", "interno": False},
    "120363215853284263@g.us": {"nome": "Zurca", "interno": False},
    "120363425598150153@g.us": {"nome": "Dr. Fellipe Barbosa", "interno": False},
    "120363422131389631@g.us": {"nome": "Gestão", "interno": True},
    "120363403421546688@g.us": {"nome": "Tripa Designer", "interno": True},
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
    requests.post(
        f"{EVOLUTION_BASE_URL}/message/sendText/{EVOLUTION_INSTANCE}",
        headers={"apikey": EVOLUTION_APIKEY, "Content-Type": "application/json"},
        json={"number": numero_ou_jid, "text": texto},
        timeout=20,
    )
 
 
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
 
 
def chamar_claude(system_prompt, conteudo_usuario, imagem_base64=None):
    messages_content = []
    if imagem_base64:
        messages_content.append({
            "type": "image",
            "source": {"type": "base64", "media_type": "image/jpeg", "data": imagem_base64},
        })
    messages_content.append({"type": "text", "text": conteudo_usuario})
 
    resp = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={
            "x-api-key": ANTHROPIC_API_KEY,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        },
        json={
            "model": "claude-haiku-4-5",
            "max_tokens": 400,
            "system": system_prompt,
            "messages": [{"role": "user", "content": messages_content}],
        },
        timeout=30,
    )
    resp.raise_for_status()
    texto = resp.json()["content"][0]["text"]
    return json.loads(texto)
 
 
# --------------------------------------------------------------------------
# Parte 1: resposta automática nos grupos de cliente
# --------------------------------------------------------------------------
 
SYSTEM_PROMPT_ATENDIMENTO = """Você redige mensagens de WhatsApp em nome da KingKong Filmes, uma agência de
marketing digital, respondendo clientes que mandam pedidos ou dúvidas em grupos de WhatsApp.
A agência atende três tipos de demanda: pedidos de arte (peças gráficas), pedidos de gravação
(vídeos/filmagens) e dúvidas gerais (status, prazos, etc). A empresa só presta atendimento,
nunca tenta vender nada na resposta.
 
TOM: sempre formal e super amigável ao mesmo tempo. Trate o cliente pelo nome quando disponível.
Nunca soe como um robô: nunca repita a mesma frase pronta - sempre reformule com suas próprias
palavras mantendo o espírito. Sempre que possível, referencie algo específico do que o cliente
mandou (o que a foto mostra, o que ele disse no áudio) em vez de um "recebemos sua mensagem"
genérico - isso é o que faz a resposta parecer atenção humana de verdade. Sem assinatura no
final. Quando mencionar quem vai cuidar da demanda, diga sempre "a equipe" (nunca nomes
específicos). Não prometa prazos ou valores exatos. No máximo 1-2 emojis, só quando fizer sentido.
 
HORÁRIO COMERCIAL: segunda a sexta, das 8h às 18h (horário de Brasília). Você vai receber a
informação se a mensagem chegou dentro ou fora desse horário.
- Dentro do horário: responda confirmando que a demanda foi recebida e será encaminhada à equipe.
- Fora do horário: avise educadamente que está fora do expediente, mas garanta que a mensagem
  foi registrada e será repassada à equipe assim que o expediente for retomado no próximo dia útil.
 
Além da resposta ao cliente, avalie se a mensagem parece de um cliente CHATEADO, FRUSTRADO,
IRRITADO ou com um tom de URGÊNCIA/RECLAMAÇÃO real (não confunda "queria saber se já está pronto"
neutro com estar chateado - só marque como chateado se houver sinal real de insatisfação,
reclamação, ou urgência forte).
 
Responda SEMPRE E APENAS em JSON válido, neste formato exato, sem nenhum texto fora do JSON:
{
  "tipo": "arte" | "gravacao" | "duvida" | "outro",
  "resposta_cliente": "texto da mensagem a ser enviada de volta ao cliente no grupo",
  "chateado": true ou false,
  "resumo_interno": "uma frase curta resumindo a mensagem do cliente, pra uso interno da equipe"
}
"""
 
 
def processar_mensagem_grupo(remote_jid, grupo, key, data):
    sender_name = data.get("pushName", "cliente")
    message = data.get("message", {})
    message_type = data.get("messageType", "")
 
    conteudo_texto = None
    imagem_base64 = None
 
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
    else:
        return {"skipped": f"tipo de mensagem não tratado: {message_type}"}
 
    if not conteudo_texto:
        return {"skipped": "sem conteúdo pra processar"}
 
    dentro_horario = dentro_do_horario_comercial()
    prompt_usuario = (
        f"Nome do cliente: {sender_name}\n"
        f"Grupo: {grupo['nome']}\n"
        f"Está dentro do horário comercial agora? {'sim' if dentro_horario else 'não'}\n"
        f"Mensagem do cliente: {conteudo_texto}"
    )
 
    resultado = chamar_claude(SYSTEM_PROMPT_ATENDIMENTO, prompt_usuario, imagem_base64=imagem_base64)
 
    resposta_cliente = resultado.get("resposta_cliente", "")
    if resposta_cliente:
        enviar_texto(remote_jid, resposta_cliente)
 
    if resultado.get("chateado"):
        alerta = (
            f"🚨 Cliente possivelmente insatisfeito!\n"
            f"Grupo: {grupo['nome']}\n"
            f"Cliente: {sender_name}\n"
            f"Resumo: {resultado.get('resumo_interno', conteudo_texto)}"
        )
        for numero in TEAM_NUMBERS:
            enviar_texto(numero, alerta)
 
    return {"resultado": resultado}
 
 
# --------------------------------------------------------------------------
# Parte 2: lembretes pessoais (Torres / Luan)
# --------------------------------------------------------------------------
 
SYSTEM_PROMPT_LEMBRETE = """O usuário está falando em português, num DM de WhatsApp com um assistente
que agenda lembretes. A data/hora atual é: {agora_iso} (horário de Brasília, America/Bahia).
 
Decida se a mensagem é um PEDIDO DE NOVO LEMBRETE (ex: "me lembra de ligar pro cliente X às 15h",
"lembra eu de mandar o orçamento amanhã de manhã") ou OUTRA COISA (uma resposta a um lembrete
anterior, uma pergunta, um comentário qualquer).
 
Responda SEMPRE E APENAS em JSON válido, neste formato exato:
{
  "eh_pedido_de_lembrete": true ou false,
  "data_hora_alvo_iso": "2026-08-29T15:00:00-03:00 (formato ISO 8601 com o fuso -03:00, só
                          preencha se eh_pedido_de_lembrete for true; interprete horários
                          relativos ao 'agora' informado acima)",
  "texto_lembrete": "um resumo curto e claro do que a pessoa quer ser lembrada de fazer"
}
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
 
 
def processar_dm(remote_jid, data):
    if numero_bate(remote_jid, TORRES_NUMBER):
        pessoa, numero = "torres", TORRES_NUMBER
    elif numero_bate(remote_jid, LUAN_NUMBER):
        pessoa, numero = "luan", LUAN_NUMBER
    else:
        return {"skipped": "DM de número não reconhecido"}
 
    message = data.get("message", {})
    texto = message.get("conversation", "")
    if not texto:
        return {"skipped": "DM sem texto (áudio/imagem em DM não tratado nesta versão)"}
 
    # Se já existe lembrete pendente pra essa pessoa, qualquer resposta encerra o nag.
    tinha_pendente = marcar_resolvido(pessoa)
 
    agora = horario_bahia_agora()
    prompt_sistema = SYSTEM_PROMPT_LEMBRETE.format(agora_iso=agora.isoformat())
    try:
        resultado = chamar_claude(prompt_sistema, texto)
    except Exception as e:
        return {"erro_claude": str(e), "lembrete_anterior_resolvido": tinha_pendente}
 
    if resultado.get("eh_pedido_de_lembrete"):
        try:
            alvo = datetime.fromisoformat(resultado["data_hora_alvo_iso"])
        except Exception:
            enviar_texto(numero, "Entendi que você quer um lembrete, mas não consegui identificar o horário certinho. Pode me falar de novo com a hora?")
            return {"erro": "não conseguiu parsear data_hora_alvo_iso", "resultado": resultado}
        agendar_lembrete(pessoa, numero, alvo, resultado.get("texto_lembrete", texto))
        enviar_texto(numero, f"Combinado! Vou te lembrar 10 min antes: \"{resultado.get('texto_lembrete', texto)}\" 👍")
    elif tinha_pendente:
        enviar_texto(numero, "Combinado, marquei como resolvido! ✅")
 
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
        if remote_jid in GRUPOS:
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
            resultado = processar_dm(remote_jid, data)
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
