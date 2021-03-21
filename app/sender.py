import psycopg2
import redis
import json
import os
from bottle import Bottle, request

class Sender(Bottle):
  def __init__(self):
    super().__init__()
    self.route('/', methods='POST', callback=self.send)
    redis_host = os.getenv('REDIS_HOST', 'queue')
    self.fila = redis.StrictRedis(host=redis_host, port=6379, db=0)
    db_host = ps.getenv('DB_HOST','db')
    db_user = ps.getenv('DB_USER','postgres')
    db_name = ps.getenv('DB_NAME','sender')
    dsn = f'dbname={db_name} user={db_user} host={db_host}'
    self.conn = psycopg2.connect(dsn)

  def register_message(self, assunto, mensagem):
    SQL = 'INSERT INTO emails (assunto, mensagem) VALUES (%s, %s)'
    cur = self.conn.cursor()
    cur.execute(SQL, (assunto, mensagem))
    self.conn.commit()
    cur.close()
    msg = {'assunto': assunto, 'mensagem': mensagem}
    self.fila.rpush('sender', json.dumps(msg))
    print('Mensagem registrada!')

  def send(self):
    assunto = request.forms.get('assunto')
    mensagem = request.forms.get('mensagem')
    self.register_message(assunto, mensagem)
    return 'Mensagem enfileirada ! Assunto {} Mensagem: {}'.format(
      assunto, mensagem
    )

if __name__ == '__main__':
  sender = Sender()
  sender.run(host='0.0.0.0', port=8080, debug=True)