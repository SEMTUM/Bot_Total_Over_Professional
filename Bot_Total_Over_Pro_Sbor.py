import json
import requests
from datetime import datetime, timedelta
import telebot
import time
from threading import Thread
import logging
from logging.handlers import RotatingFileHandler
import math
import statistics
from collections import Counter
import pandas as pd
import os

# ==============================================
# НАСТРОЙКА ЛОГИРОВАНИЯ
# ==============================================
def setup_logging():
    """Настройка системы логирования"""
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # Формат сообщений
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    
    # Обработчик для файла (ротация по 5 МБ)
    file_handler = RotatingFileHandler(
        'bot.log', 
        maxBytes=5*1024*1024, 
        backupCount=3,
        encoding='utf-8'
    )
    file_handler.setFormatter(formatter)
    
    # Обработчик для консоли
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    
    # Добавляем обработчики
    logger.handlers = [file_handler, console_handler]
    
    # Отключаем DEBUG логи для всех модулей
    logging.getLogger('telebot').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)

setup_logging()
logger = logging.getLogger(__name__)

# ==============================================
# КОНФИГУРАЦИЯ BOT TOTAL OVER PRO SBOR
# ==============================================
headers = {"x-fsign": "SW9D1eZo"}  # Ключ авторизации Flashscore

# НАСТРАИВАЕМЫЕ ПАРАМЕТРЫ
MIN_AVG_PROBABILITY = 0.01      # Минимальная средняя вероятность
MIN_BOOKMAKER_ODDS = 1.70       # Минимальный коэффициент букмекера
MAX_BOOKMAKER_ODDS = 100.00       # Максимальный коэффициент букмекера
MIN_OUR_ODDS = 1.00             # Минимальный наш ожидаемый коэффициент
MAX_OUR_ODDS = 100.00           # Максимальный наш ожидаемый коэффициент
STAT_DB = True                  # Включить сбор статистики в базу данных

# ВКЛЮЧЕНИЕ/ОТКЛЮЧЕНИЕ МЕТОДОВ РАСЧЕТА
ENABLED_METHODS = {
    'poisson': (True, 10),              # 🥇 Обязательно Пуасон ЗОЛОТОЙ СТАНДАРТ - основа прогнозирования
    'weighted_poisson': (True, 10),     # 🥇 Обязательно Взвешенный Пуассон Улучшенная версия - учитывает д/г
    'attacking_potential': (True, 10),  # 🥇 Обязательно Атакующий потенциал Ключевой - баланс атаки/защиты
    'bayesian': (True, 10),             # 🥇 Обязательно Байесовский подход Адаптивный - обновляет вероятности
    'historical_totals': (True, 10),    # 🥈 Рекомендуется Исторические тоталы Хороший базовый индикатор тоталов
    'recent_form': (True, 5),           # 🥈 Рекомендуется Форма последних матчей Важен для текущей формы команд
    'ml_approach': (True, 10)           # 🥈 Рекомендуется ML подход Упрощенный ML дает неплохие результаты
}

# ТЕЛЕГРАМ ТОКЕН И АДМИНИСТРАТОР
TEL_TOKEN = "8256717454:AAG-mw9JyOqMbX-tNzZ-GriaDMlxn5Zyof8"  # Токен бота
ID_ADMIN = "627946014"  # ID администратора (принудительно)

# НАСТРОЙКИ ПОВТОРНЫХ ПОПЫТОК И ЛИМИТОВ
MAX_RETRY_ATTEMPTS = 30                    # Количество повторных попыток
RETRY_DELAY = 60                           # Увеличиваем задержку между попытками
API_RETRY_DELAY = 60                       # Задержка при критических ошибках API
POLLING_TIMEOUT = 90                       # Увеличиваем таймаут для polling
CLEANUP_DAYS = 2                           # Удаляем матчи из списка через 2 дня
TELEGRAM_RATE_LIMIT_DELAY = 2              # Задержка между отправками в Telegram
MAX_MESSAGES_PER_MINUTE = 20               # Лимит сообщений в минуту

# ==============================================
# ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ
# ==============================================
sent_matches = []  # Хранит информацию об отправленных матчах
active_chats = {}  # Словарь активных чатов {chat_id: last_activity}
admin_chat_id = int(ID_ADMIN) if ID_ADMIN else None  # ID администратора (принудительно)
message_ids = {}  # Хранит ID сообщений для каждого матча {match_key: {chat_id: message_id}}

# ==============================================
# КЛАСС ДЛЯ УПРАВЛЕНИЯ ЛИМИТАМИ TELEGRAM
# ==============================================
class TelegramRateLimiter:
    """Класс для управления лимитами отправки сообщений в Telegram"""
    
    def __init__(self):
        self.last_sent_time = 0
        self.message_count = 0
        self.minute_start = time.time()
    
    def wait_if_needed(self):
        """Ожидает если достигнут лимит сообщений"""
        current_time = time.time()
        
        # Сбрасываем счетчик каждую минуту
        if current_time - self.minute_start >= 60:
            self.message_count = 0
            self.minute_start = current_time
        
        # Проверяем лимит сообщений в минуту
        if self.message_count >= MAX_MESSAGES_PER_MINUTE:
            sleep_time = 60 - (current_time - self.minute_start)
            if sleep_time > 0:
                logger.warning(f"Достигнут лимит сообщений. Ожидание {sleep_time:.1f} секунд")
                time.sleep(sleep_time)
                self.message_count = 0
                self.minute_start = time.time()
        
        # Базовая задержка между сообщениями
        time_since_last = current_time - self.last_sent_time
        if time_since_last < TELEGRAM_RATE_LIMIT_DELAY:
            time.sleep(TELEGRAM_RATE_LIMIT_DELAY - time_since_last)
        
        self.last_sent_time = time.time()
        self.message_count += 1

# Создаем глобальный объект для управления лимитами
rate_limiter = TelegramRateLimiter()

# ==============================================
# КЛАСС ДЛЯ РАБОТЫ С БАЗОЙ ДАННЫХ
# ==============================================
class StatisticsDB:
    """Класс для работы с базой данных статистики"""
    
    DB_FILENAME = "bot_statistics.xlsx"
    
    @staticmethod
    def initialize_database():
        """Инициализирует базу данных, создает файл если его нет"""
        if not STAT_DB:
            return
        
        try:
            if not os.path.exists(StatisticsDB.DB_FILENAME):
                # Создаем новую базу данных с нужными колонками
                df = pd.DataFrame(columns=[
                    'Дата',
                    'Матч', 
                    'poisson',
                    'weighted_poisson',
                    'attacking_potential',
                    'bayesian',
                    'historical_totals',
                    'recent_form',
                    'ml_approach',
                    'Вероятность',
                    'Наш Кэф',
                    'Кэф БК',
                    'Счет',
                    'Зашла/Не зашла'
                ])
                df.to_excel(StatisticsDB.DB_FILENAME, index=False)
                logger.info(f"Создана новая база данных: {StatisticsDB.DB_FILENAME}")
            else:
                logger.info(f"База данных уже существует: {StatisticsDB.DB_FILENAME}")
                
        except Exception as e:
            logger.error(f"Ошибка при инициализации базы данных: {str(e)}")
    
    @staticmethod
    def add_completed_match_to_db(match_data):
        """Добавляет информацию о завершенном матче в базу данных"""
        if not STAT_DB:
            return
        
        try:
            # Проверяем, что матч завершен и есть результат
            if not match_data.get('result') or match_data.get('status') != 'finished':
                logger.debug(f"Матч не завершен, пропускаем добавление в базу: {match_data.get('teams')}")
                return
            
            # Проверяем существование файла
            if not os.path.exists(StatisticsDB.DB_FILENAME):
                StatisticsDB.initialize_database()
            
            # Загружаем существующие данные
            df = pd.read_excel(StatisticsDB.DB_FILENAME)
            
            # Проверяем, нет ли уже этого матча в базе
            existing_match = df[(df['Дата'] == match_data['date_str']) & 
                               (df['Матч'] == match_data['teams'])]
            
            if not existing_match.empty:
                logger.debug(f"Матч уже существует в базе данных: {match_data['teams']}")
                return
            
            # Определяем результат ставки
            if match_data['result'] and ':' in match_data['result']:
                home_score, away_score = map(int, match_data['result'].split(':'))
                total_goals = home_score + away_score
                bet_result = "Зашла" if total_goals >= 3 else "Не зашла"
            else:
                bet_result = "Неизвестно"
            
            # Получаем вероятности по методам из данных матча
            method_probabilities = {}
            for method_name in ['poisson', 'weighted_poisson', 'attacking_potential', 'bayesian', 'historical_totals', 'recent_form', 'ml_approach']:
                method_prob = match_data.get(f'{method_name}_prob')
                if method_prob is not None:
                    method_probabilities[method_name] = method_prob
                else:
                    method_probabilities[method_name] = 0
                    logger.warning(f"Вероятность для метода {method_name} не найдена в данных матча")
            
            # Подготавливаем данные для добавления
            db_match_data = {
                'Дата': match_data['date_str'],
                'Матч': match_data['teams'],
                'poisson': method_probabilities['poisson'],
                'weighted_poisson': method_probabilities['weighted_poisson'],
                'attacking_potential': method_probabilities['attacking_potential'],
                'bayesian': method_probabilities['bayesian'],
                'historical_totals': method_probabilities['historical_totals'],
                'recent_form': method_probabilities['recent_form'],
                'ml_approach': method_probabilities['ml_approach'],
                'Вероятность': match_data['avg_probability'],
                'Наш Кэф': match_data['our_expected_odds'],
                'Кэф БК': match_data['odds'],
                'Счет': match_data['result'],
                'Зашла/Не зашла': bet_result
            }
            
            # Добавляем новую запись
            new_row = pd.DataFrame([db_match_data])
            df = pd.concat([df, new_row], ignore_index=True)
            
            # Сохраняем обратно в файл
            df.to_excel(StatisticsDB.DB_FILENAME, index=False)
            logger.info(f"Добавлен завершенный матч в базу данных: {match_data['teams']} - {match_data['result']}")
            
        except Exception as e:
            logger.error(f"Ошибка при добавлении матча в базу данных: {str(e)}")

# ==============================================
# ОБНОВЛЕННЫЙ КЛАСС ДЛЯ РАБОТЫ С API FLASHSCORE С ПОВТОРНЫМИ ПОПЫТКАМИ
# ==============================================
class FlashscoreAPI:
    """Класс для работы с API Flashscore с улучшенной обработкой ошибок"""
    
    @staticmethod
    def make_request_with_retry(url, timeout=15):  # Увеличиваем таймаут
        """Выполняет запрос с повторными попытками при ошибках сети"""
        for attempt in range(MAX_RETRY_ATTEMPTS):
            try:
                response = requests.get(url=url, headers=headers, timeout=timeout)
                response.raise_for_status()
                return response
            except requests.exceptions.Timeout:
                logger.warning(f"Таймаут попытка {attempt + 1}/{MAX_RETRY_ATTEMPTS} для {url}")
            except requests.exceptions.SSLError as e:
                logger.warning(f"SSL ошибка попытка {attempt + 1}/{MAX_RETRY_ATTEMPTS} для {url}: {str(e)}")
            except requests.exceptions.ConnectionError as e:
                logger.warning(f"Ошибка соединения попытка {attempt + 1}/{MAX_RETRY_ATTEMPTS} для {url}: {str(e)}")
            except requests.exceptions.RequestException as e:
                logger.warning(f"Ошибка запроса попытка {attempt + 1}/{MAX_RETRY_ATTEMPTS} для {url}: {str(e)}")
            
            if attempt < MAX_RETRY_ATTEMPTS - 1:
                sleep_time = RETRY_DELAY * (attempt + 1)  # Увеличиваем задержку с каждой попыткой
                logger.info(f"Повторная попытка через {sleep_time} секунд...")
                time.sleep(sleep_time)
            else:
                logger.error(f"Все {MAX_RETRY_ATTEMPTS} попыток не удались для {url}")
                return None
        return None

    @staticmethod
    def get_matches(day):
        """Получает список матчей на указанный день с обработкой ошибок"""
        try:
            feed = f'f_1_{day}_3_ru_5'
            url = f'https://d.flashscorekz.com/x/feed/{feed}'
            
            response = FlashscoreAPI.make_request_with_retry(url)
            if response is None:
                return []
                
            data = response.text.split('¬')
            data_list = [{}]
            list_match = []

            # Парсим данные
            for item in data:
                if '÷' not in item:
                    continue
                key = item.split('÷')[0]
                value = item.split('÷')[-1]

                if '~' in key:
                    data_list.append({key: value})
                else:
                    data_list[-1].update({key: value})

            # Формируем список матчей
            league = "Неизвестно"
            for game in data_list:
                if not game:
                    continue
                    
                if 'ZA' in list(game.keys())[0]:
                    league = game.get("~ZA")
                if 'AA' in list(game.keys())[0]:
                    date = datetime.fromtimestamp(int(game.get("AD")))
                    team_1 = game.get("AE")
                    team_2 = game.get("AF")
                    id_match = game.get("~AA")
                    list_match.append([id_match, team_1, team_2, date, league])
            
            return list_match
        
        except Exception as e:
            logger.error(f"Ошибка при получении списка матчей: {str(e)}")
        return []

    @staticmethod
    def get_total_goals(match_id):
        """Получает статистику по голам для матча с обработкой ошибок"""
        try:
            url = f'https://46.flashscore.ninja/46/x/feed/df_hh_1_{match_id}'
            
            response = FlashscoreAPI.make_request_with_retry(url)
            if response is None:
                return None
                
            data = response.text.split('¬')
            data_list = [{}]
            list_match = []

            # Парсим данные
            for item in data:
                if '÷' not in item:
                    continue
                    
                key = item.split('÷')[0]
                value = item.split('÷')[-1]

                if '~' in key:
                    data_list.append({key: value})
                else:
                    data_list[-1].update({key: value})

            # Формируем структуру данных
            for item in data_list:
                if not item:
                    continue
                    
                if '~KB' in item.keys():
                    list_match.append([item.get('~KB')])
                    list_match[-1].append([])
                if 'KJ' in item.keys():
                    list_match[-1][-1].append(item.get('KJ'))
                if 'KK' in item.keys():
                    list_match[-1][-1].append(item.get('KK'))
                if 'KU' in item.keys() and item.get('KU') != '':
                    list_match[-1][-1].append(int(item.get('KU')))
                if 'KT' in item.keys() and item.get('KT') != '':
                    list_match[-1][-1].append(int(item.get('KT')))
                if 'KS' in item.keys():
                    list_match[-1][-1].append(item.get('KS'))
                    list_match[-1].append([])
            
            return list_match
        
        except Exception as e:
            logger.error(f"Ошибка при получении статистики для матча {match_id}: {str(e)}")
        return None

    @staticmethod
    def get_odds(match_id):
        """Получает коэффициенты на матч с обработкой ошибок"""
        try:
            url = f'https://global.ds.lsapp.eu/odds/pq_graphql?_hash=oce&eventId={match_id}&projectId=46&geoIpCode=RU&geoIpSubdivisionCode=RU'
            
            response = FlashscoreAPI.make_request_with_retry(url)
            if response is None:
                return None
                
            data = json.loads(response.text)['data']['findOddsByEventId']['odds']
            
            for item in data:
                if item['bettingType'] == 'OVER_UNDER' and item['bettingScope'] == 'FULL_TIME':
                    odds = item['odds']
                    for elem in odds:
                        if float(elem['handicap']['value']) == 2.5 and elem['selection'] == 'OVER': 
                            return elem['value']
            return None
            
        except (KeyError, json.JSONDecodeError) as e:
            logger.error(f"Ошибка формата данных коэффициентов для матча {match_id}: {str(e)}")
        except Exception as e:
            logger.error(f"Ошибка при обработке коэффициентов для матча {match_id}: {str(e)}")
        return None

    @staticmethod
    def get_match_result(match_id):
        """Получает текущий результат и статус матча с обработкой ошибок"""
        try:
            url = f'https://46.flashscore.ninja/46/x/feed/dc_1_{match_id}'
            
            response = FlashscoreAPI.make_request_with_retry(url)
            if response is None:
                return None, None, None, None
                
            data = response.text.split('¬')
            
            score = None
            score_1, score_2 = None, None
            status = None
            home_score = None
            away_score = None
            
            for item in data:
                if '÷' not in item:
                    continue
                    
                if 'DG÷' in item:  # Текущий счет
                    score_1 = item.split('÷')[1]
                if 'DH÷' in item:
                    score_2 = item.split('÷')[1]
                if 'DA÷3' in item:
                    status = 'finished'
                elif 'DA÷1' in item:
                    status = '1st half'
                elif 'DA÷2' in item:
                    status = '2nd half'
                elif 'DA÷4' in item:
                    status = 'halftime'
                elif 'DA÷0' in item:
                    status = 'upcoming'
                    
            if score_1 is not None and score_2 is not None:
                score = f"{score_1}:{score_2}"
                home_score, away_score = int(score_1), int(score_2)
            
            return score, status, home_score, away_score
        
        except Exception as e:
            logger.error(f"Ошибка при получении результата матча {match_id}: {str(e)}")
            return None, None, None, None

# ==============================================
# РАСШИРЕННЫЕ МЕТОДЫ РАСЧЕТА ВЕРОЯТНОСТЕЙ
# ==============================================
class ProbabilityCalculator:
    """Класс для расчета вероятностей различными методами"""
    
    @staticmethod
    def calculate_poisson_probability(lambda_param, k):
        """Распределение Пуассона"""
        try:
            return (math.exp(-lambda_param) * (lambda_param ** k)) / math.factorial(k)
        except:
            return 0

    @staticmethod
    def advanced_probability_analysis(team1_data, team2_data):
        """Расширенный анализ вероятностей"""
        
        def poisson_method():
            """Метод 1: Пуассоновское распределение"""
            enabled, min_matches = ENABLED_METHODS['poisson']
            if not enabled:
                return 0, "Пуассон"
            if team1_data['matches_processed'] < min_matches or team2_data['matches_processed'] < min_matches:
                logger.debug(f"Пропуск метода Пуассон: недостаточно матчей ({team1_data['matches_processed']}/{team2_data['matches_processed']} < {min_matches})")
                return 0, "Пуассон"
                
            lambda1 = team1_data['total_goals_scored'] / team1_data['matches_processed']
            lambda2 = team2_data['total_goals_scored'] / team2_data['matches_processed']
            total_lambda = (lambda1 + lambda2) / 2
            
            prob_over_25 = sum(ProbabilityCalculator.calculate_poisson_probability(total_lambda * 2.5, k) 
                             for k in range(3, 15))
            return min(prob_over_25, 0.95), "Пуассон"

        def weighted_poisson():
            """Метод 2: Взвешенный Пуассон"""
            enabled, min_matches = ENABLED_METHODS['weighted_poisson']
            if not enabled:
                return 0, "Взвеш.Пуассон"
            if team1_data['matches_processed'] < min_matches or team2_data['matches_processed'] < min_matches:
                logger.debug(f"Пропуск метода Взвешенный Пуассон: недостаточно матчей ({team1_data['matches_processed']}/{team2_data['matches_processed']} < {min_matches})")
                return 0, "Взвеш.Пуассон"
                
            home_weight = 1.2
            away_weight = 0.8
            
            lambda1_home = sum(m['goals_scored'] for m in team1_data['matches_details'] if m['type'] == 'дома') / max(team1_data['home_matches'], 1)
            lambda1_away = sum(m['goals_scored'] for m in team1_data['matches_details'] if m['type'] == 'в гостях') / max(team1_data['away_matches'], 1)
            lambda2_home = sum(m['goals_scored'] for m in team2_data['matches_details'] if m['type'] == 'дома') / max(team2_data['home_matches'], 1)
            lambda2_away = sum(m['goals_scored'] for m in team2_data['matches_details'] if m['type'] == 'в гостях') / max(team2_data['away_matches'], 1)
            
            weighted_lambda = (lambda1_home * home_weight + lambda2_away * away_weight + 
                            lambda2_home * home_weight + lambda1_away * away_weight) / 4
            
            prob_over_25 = sum(ProbabilityCalculator.calculate_poisson_probability(weighted_lambda * 2.5, k) 
                             for k in range(3, 15))
            return min(prob_over_25, 0.95), "Взвеш.Пуассон"

        def historical_totals():
            """Метод 3: Исторические тоталы"""
            enabled, min_matches = ENABLED_METHODS['historical_totals']
            if not enabled:
                return 0, "Историч.тотал"
            if team1_data['matches_processed'] < min_matches or team2_data['matches_processed'] < min_matches:
                logger.debug(f"Пропуск метода Исторические тоталы: недостаточно матчей ({team1_data['matches_processed']}/{team2_data['matches_processed']} < {min_matches})")
                return 0, "Историч.тотал"
                
            all_totals = [m['total_goals'] for m in team1_data['matches_details']] + \
                        [m['total_goals'] for m in team2_data['matches_details']]
            
            over_25_count = sum(1 for total in all_totals if total > 2.5)
            prob = over_25_count / len(all_totals) if all_totals else 0.5
            return prob, "Историч.тотал"

        def recent_form():
            """Метод 4: Форма последних матчей"""
            enabled, min_matches = ENABLED_METHODS['recent_form']
            if not enabled:
                return 0, "Форма"
            if team1_data['matches_processed'] < min_matches or team2_data['matches_processed'] < min_matches:
                logger.debug(f"Пропуск метода Форма: недостаточно матчей ({team1_data['matches_processed']}/{team2_data['matches_processed']} < {min_matches})")
                return 0, "Форма"
                
            recent_matches = team1_data['matches_details'][-5:] + team2_data['matches_details'][-5:]
            over_25_count = sum(1 for m in recent_matches if m['total_goals'] > 2.5)
            prob = over_25_count / len(recent_matches) if recent_matches else 0.5
            return prob, "Форма"

        def attacking_potential():
            """Метод 5: Атакующий потенциал"""
            enabled, min_matches = ENABLED_METHODS['attacking_potential']
            if not enabled:
                return 0, "Атака"
            if team1_data['matches_processed'] < min_matches or team2_data['matches_processed'] < min_matches:
                logger.debug(f"Пропуск метода Атакующий потенциал: недостаточно матчей ({team1_data['matches_processed']}/{team2_data['matches_processed']} < {min_matches})")
                return 0, "Атака"
                
            attack1 = team1_data['total_goals_scored'] / team1_data['matches_processed']
            attack2 = team2_data['total_goals_scored'] / team2_data['matches_processed']
            defense1 = team1_data['total_goals_conceded'] / team1_data['matches_processed']
            defense2 = team2_data['total_goals_conceded'] / team2_data['matches_processed']
            
            expected_goals = (attack1 + defense2 + attack2 + defense1) / 2
            prob = min(0.95, expected_goals / 3.5)
            return prob, "Атака"

        def bayesian_method():
            """Метод 6: Байесовский подход"""
            enabled, min_matches = ENABLED_METHODS['bayesian']
            if not enabled:
                return 0, "Байесовский"
            if team1_data['matches_processed'] < min_matches or team2_data['matches_processed'] < min_matches:
                logger.debug(f"Пропуск метода Байесовский: недостаточно матчей ({team1_data['matches_processed']}/{team2_data['matches_processed']} < {min_matches})")
                return 0, "Байесовский"
                
            prior_alpha = 8
            prior_beta = 8
            
            total_goals = team1_data['total_goals_scored'] + team2_data['total_goals_scored']
            total_matches = team1_data['matches_processed'] + team2_data['matches_processed']
            
            posterior_alpha = prior_alpha + total_goals
            posterior_beta = prior_beta + total_matches
            
            expected_goals = posterior_alpha / posterior_beta
            prob = min(0.95, expected_goals / 3.0)
            return prob, "Байесовский"

        def ml_approach():
            """Метод 7: Машинное обучение (упрощенное)"""
            enabled, min_matches = ENABLED_METHODS['ml_approach']
            if not enabled:
                return 0, "ML подход"
            if team1_data['matches_processed'] < min_matches or team2_data['matches_processed'] < min_matches:
                logger.debug(f"Пропуск метода ML подход: недостаточно матчей ({team1_data['matches_processed']}/{team2_data['matches_processed']} < {min_matches})")
                return 0, "ML подход"
                
            features = []
            features.append(team1_data['total_goals_scored'] / team1_data['matches_processed'])
            features.append(team2_data['total_goals_scored'] / team2_data['matches_processed'])
            features.append(team1_data['total_goals_conceded'] / team1_data['matches_processed'])
            features.append(team2_data['total_goals_conceded'] / team2_data['matches_processed'])
            
            tb1 = sum(1 for m in team1_data['matches_details'] if m['total_goals'] > 2.5) / team1_data['matches_processed']
            tb2 = sum(1 for m in team2_data['matches_details'] if m['total_goals'] > 2.5) / team2_data['matches_processed']
            features.extend([tb1, tb2])
            
            # Простое усреднение
            prob = min(0.95, sum(features) / len(features))
            return prob, "ML подход"

        # Запускаем все включенные методы
        methods = [
            poisson_method, weighted_poisson, historical_totals, recent_form,
            attacking_potential, bayesian_method, ml_approach
        ]
        
        results = []
        all_methods_sufficient = True
        
        for method in methods:
            try:
                prob, name = method()
                if prob > 0:  # Добавляем только включенные методы с достаточным количеством матчей
                    results.append((name, prob))
                else:
                    # Если метод включен, но недостаточно данных - отмечаем это
                    enabled, min_matches = ENABLED_METHODS[method.__name__.replace('_method', '')]
                    if enabled:
                        all_methods_sufficient = False
                        logger.debug(f"Метод {name} пропущен из-за недостатка данных")
            except Exception as e:
                logger.warning(f"Ошибка в методе {method.__name__}: {str(e)}")
                continue
        
        # Если хотя бы один включенный метод не может быть применен из-за недостатка данных - пропускаем матч
        if not all_methods_sufficient:
            logger.debug(f"Пропуск матча: недостаточно данных хотя бы для одного включенного метода")
            return []
        
        return results

# ==============================================
# ФИЛЬТР КАЧЕСТВА ДАННЫХ
# ==============================================
def is_data_quality_sufficient(team1_data, team2_data):
    """
    Проверка достаточности качества данных для анализа
    Матч пропускается если недостаточно данных хотя бы для одного включенного метода
    """
    # Проверяем все включенные методы
    for method_name, (enabled, min_matches) in ENABLED_METHODS.items():
        if enabled:
            # Если метод включен, но недостаточно данных для него - матч пропускается
            if team1_data['matches_processed'] < min_matches or team2_data['matches_processed'] < min_matches:
                logger.debug(f"Пропуск матча: недостаточно данных для метода {method_name} ({team1_data['matches_processed']}/{team2_data['matches_processed']} < {min_matches})")
                return False
    
    return True

def extract_team_data(detail, team_name):
    """Извлекает статистические данные команды"""
    matches_details = []
    total_goals_scored = 0
    total_goals_conceded = 0
    home_matches = 0
    away_matches = 0
    
    for j in range(1, min(11, len(detail))):  # Изменено с 6 на 11 для извлечения 10 матчей
        try:
            if len(detail[j]) < 5:
                continue
                
            if detail[j][4] == 'home':
                goals_scored = detail[j][2]
                goals_conceded = detail[j][3]
                match_type = "дома"
                home_matches += 1
            elif detail[j][4] == 'away':
                goals_scored = detail[j][3]
                goals_conceded = detail[j][2]
                match_type = "в гостях"
                away_matches += 1
            else:
                continue
                
            matches_details.append({
                'type': match_type,
                'goals_scored': goals_scored,
                'goals_conceded': goals_conceded,
                'total_goals': goals_scored + goals_conceded
            })
            
            total_goals_scored += goals_scored
            total_goals_conceded += goals_conceded
            
        except (IndexError, TypeError):
            continue
    
    if not matches_details:
        return None
    
    return {
        'matches_processed': len(matches_details),
        'matches_details': matches_details,
        'total_goals_scored': total_goals_scored,
        'total_goals_conceded': total_goals_conceded,
        'home_matches': home_matches,
        'away_matches': away_matches
    }

def create_methods_visualization(probability_results):
    """Создает визуализацию результатов методов в виде эмодзи"""
    visualization = []
    for name, prob in probability_results:
        prob_percent = prob * 100
        if prob_percent >= 80:
            visualization.append("🟢")
        elif prob_percent >= 60:
            visualization.append("🟡")
        else:
            visualization.append("🔴")
    return "".join(visualization)

# ==============================================
# ФУНКЦИИ АНАЛИЗА МАТЧЕЙ
# ==============================================
def analyze_matches():
    """Анализирует матчи и возвращает подходящие"""
    global sent_matches
    
    try:
        # Очищаем старые матчи
        current_time = datetime.now()
        sent_matches = [match for match in sent_matches 
                       if (current_time - match['match_time']) <= timedelta(days=CLEANUP_DAYS)]
        
        list_live = []
        search_list = FlashscoreAPI.get_matches(0)  # Матчи на сегодня
        
        if not search_list:
            logger.info("Список матчей для анализа пуст")
            return []
        
        for match in search_list:
            match_id = match[0]
            
            # Пропускаем матчи, которые уже начались или завершились
            current_time = datetime.now()
            if match[3] < current_time:
                logger.debug(f"Пропускаем матч {match_id} - уже начался или завершился")
                continue
            
            # Получаем статистику по матчу
            detail = FlashscoreAPI.get_total_goals(str(match_id))
            if not detail or len(detail) < 2 or len(detail[0]) < 7 or len(detail[1]) < 7:
                logger.debug(f"Недостаточно статистики для матча {match_id}")
                continue

            # Извлекаем статистику команд
            team1_data = extract_team_data(detail[0], match[1])
            team2_data = extract_team_data(detail[1], match[2])
            
            if not team1_data or not team2_data:
                logger.debug(f"Не удалось извлечь данные команд для матча {match_id}")
                continue

            # ПРОВЕРКА КАЧЕСТВА ДАННЫХ - если недостаточно данных хотя бы для одного метода, пропускаем матч
            if not is_data_quality_sufficient(team1_data, team2_data):
                continue

            # Рассчитываем вероятности всеми методами
            probability_results = ProbabilityCalculator.advanced_probability_analysis(team1_data, team2_data)
            
            # Если probability_results пустой список - значит недостаточно данных для какого-то метода
            if not probability_results:
                logger.debug(f"Пропуск матча {match_id}: недостаточно данных для всех включенных методов")
                continue

            # Вычисляем среднюю вероятность
            total_prob = sum(prob for _, prob in probability_results)
            avg_probability = total_prob / len(probability_results)
            
            # Получаем коэффициент букмекера
            bookmaker_odds = FlashscoreAPI.get_odds(match_id)
            if bookmaker_odds is None:
                logger.debug(f"Не удалось получить коэффициенты для матча {match_id}")
                continue
                
            bookmaker_odds = float(bookmaker_odds)
            
            # Рассчитываем наш ожидаемый коэффициент
            our_expected_odds = 1 / avg_probability if avg_probability > 0 else 999
            
            # Проверяем фильтры
            if (avg_probability >= MIN_AVG_PROBABILITY and 
                MIN_BOOKMAKER_ODDS <= bookmaker_odds <= MAX_BOOKMAKER_ODDS and
                MIN_OUR_ODDS <= our_expected_odds <= MAX_OUR_ODDS):
                
                date_str = match[3].strftime('%d.%m.%Y')
                time_str = match[3].strftime('%H:%M')
                match_key = f"{match[1]}_{match[2]}_{date_str}"
                
                if not any(m['key'] == match_key for m in sent_matches):
                    # Создаем визуализацию методов
                    method_visualization = create_methods_visualization(probability_results)
                    
                    match_data = {
                        'key': match_key,
                        'match_id': match_id,
                        'date_str': date_str,
                        'time_str': time_str,
                        'league': match[4],
                        'teams': f"{match[1]} - {match[2]}",
                        'odds': bookmaker_odds,
                        'avg_probability': round(avg_probability * 100, 1),
                        'our_expected_odds': round(our_expected_odds, 2),
                        'methods_visualization': method_visualization,
                        'match_time': match[3],
                        'result': None,
                        'bet_status': None,
                        'status': 'upcoming'
                    }
                    
                    # Сохраняем вероятности по каждому методу для базы данных
                    method_name_mapping = {
                        'Пуассон': 'poisson',
                        'Взвеш.Пуассон': 'weighted_poisson', 
                        'Атака': 'attacking_potential',
                        'Байесовский': 'bayesian',
                        'Историч.тотал': 'historical_totals',
                        'Форма': 'recent_form',
                        'ML подход': 'ml_approach'
                    }
                    
                    for method_name, prob in probability_results:
                        db_method_name = method_name_mapping.get(method_name)
                        if db_method_name:
                            match_data[f'{db_method_name}_prob'] = prob
                        else:
                            logger.warning(f"Неизвестное имя метода: {method_name}")
                    
                    list_live.append(match_data)
        
        return list_live
    
    except Exception as e:
        logger.error(f"Критическая ошибка в analyze_matches: {str(e)}", exc_info=True)
        return []

def check_match_results():
    """Проверяет результаты матчей и обновляет карточки"""
    global sent_matches, message_ids
    
    try:
        for match in sent_matches[:]:
            try:
                score, status, home_score, away_score = FlashscoreAPI.get_match_result(match['match_id'])
                
                if status == 'finished' and match['status'] != 'finished':
                    # Определяем результат ставки
                    if home_score is not None and away_score is not None:
                        total_goals = home_score + away_score
                        bet_status = "✅ Ставка зашла!" if total_goals >= 3 else "❌ Ставка не зашла!"
                        bet_result_db = "Зашла" if total_goals >= 3 else "Не зашла"
                    else:
                        bet_status = "⚖️ Результат неизвестен"
                        bet_result_db = "Неизвестно"
                    
                    # Обновляем информацию о матче
                    match['status'] = 'finished'
                    match['result'] = score
                    match['bet_status'] = bet_status
                    
                    # Обновляем карточки у всех пользователей
                    update_match_card(match)
                    
                    # Добавляем завершенный матч в базу данных если включен сбор статистики
                    if STAT_DB and score:
                        StatisticsDB.add_completed_match_to_db(match)
                    
                    logger.info(f"Матч завершен: {match['teams']} {score} - {bet_status}")
                    
            except Exception as e:
                logger.error(f"Ошибка при проверке матча {match['match_id']}: {str(e)}", exc_info=True)
    
    except Exception as e:
        logger.error(f"Критическая ошибка в check_match_results: {str(e)}", exc_info=True)

# ==============================================
# ФУНКЦИИ РАБОТЫ С TELEGRAM
# ==============================================
def update_match_card(match):
    """Обновляет карточку матча у всех пользователей"""
    global message_ids
    
    try:
        # Форматируем коэффициенты с двумя знаками после запятой
        odds_formatted = f"{match['odds']:.2f}"
        our_odds_formatted = f"{match['our_expected_odds']:.2f}"
        
        if match['status'] == 'upcoming':
            # Новая карточка для предстоящего матча
            message_text = (
                f"🟢 Внимание! Новый прогноз!\n\n"
                f"🏆 {match['league']}\n\n"
                f"⚔️ {match['teams']}\n\n"
                f"-----------------------------------------------\n"
                f"📅 {match['date_str']} {match['time_str']}\n"
                f"-----------------------------------------------\n"
                f"{match['methods_visualization']}\n"
                f"-----------------------------------------------\n"
                f"📊 Вероятность:  {match['avg_probability']} %\n"
                f"-----------------------------------------------\n"
                f"📊 Наш Коэффициент:  {our_odds_formatted}\n"
                f"-----------------------------------------------\n\n"
                f"🔥 Прогноз: ТБ 2.5 за {odds_formatted}"
            )
        elif match['status'] == 'finished':
            # Карточка для завершенного матча
            message_text = (
                f"⚪️ Матч закончился!\n\n"
                f"🏆 {match['league']}\n\n"
                f"⚔️ {match['teams']}\n\n"
                f"-----------------------------------------------\n"
                f"📅 {match['date_str']} {match['time_str']}\n"
                f"-----------------------------------------------\n"
                f"{match['methods_visualization']}\n"
                f"-----------------------------------------------\n"
                f"📊 Вероятность:  {match['avg_probability']} %\n"
                f"-----------------------------------------------\n"
                f"📊 Наш Коэффициент:  {our_odds_formatted}\n"
                f"-----------------------------------------------\n\n"
                f"🔥 Прогноз: ТБ 2.5 за {odds_formatted}\n"
                f"{match['bet_status']} Счет {match['result']}"
            )
        
        # Обновляем сообщение у всех пользователей
        if match['key'] in message_ids:
            for chat_id, msg_id in message_ids[match['key']].items():
                try:
                    rate_limiter.wait_if_needed()
                    bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=msg_id,
                        text=message_text
                    )
                except telebot.apihelper.ApiTelegramException as e:
                    if "message to edit not found" in str(e).lower() or "chat not found" in str(e).lower():
                        logger.info(f"Чат {chat_id} больше не активен, удаляем из списка")
                        active_chats.pop(chat_id, None)
                        message_ids[match['key']].pop(chat_id, None)
                    elif "too many requests" in str(e).lower():
                        retry_after = int(str(e).split('retry after ')[-1])
                        logger.warning(f"Достигнут лимит Telegram API. Ожидание {retry_after} секунд")
                        time.sleep(retry_after)
                        # Повторяем попытку после ожидания
                        try:
                            bot.edit_message_text(
                                chat_id=chat_id,
                                message_id=msg_id,
                                text=message_text
                            )
                        except:
                            pass
                    else:
                        logger.error(f"Не удалось обновить сообщение в чат {chat_id}: {str(e)}")
                except Exception as e:
                    logger.error(f"Не удалось обновить сообщение в чат {chat_id}: {str(e)}")
    
    except Exception as e:
        logger.error(f"Ошибка при обновлении карточки матча: {str(e)}", exc_info=True)

def broadcast_message(message_text, match_key=None):
    """Отправляет или обновляет сообщение всем пользователям"""
    global message_ids
    
    try:
        if match_key:
            # Для матчей сохраняем ID сообщений для последующего обновления
            if match_key not in message_ids:
                message_ids[match_key] = {}
                
            for chat_id in list(active_chats.keys()):
                try:
                    rate_limiter.wait_if_needed()
                    sent_msg = bot.send_message(chat_id, message_text)
                    message_ids[match_key][chat_id] = sent_msg.message_id
                    active_chats[chat_id] = datetime.now()  # Обновляем время активности
                except telebot.apihelper.ApiTelegramException as e:
                    if "chat not found" in str(e).lower():
                        logger.info(f"Чат {chat_id} недоступен, удаляем из активных")
                        active_chats.pop(chat_id, None)
                        if match_key in message_ids and chat_id in message_ids[match_key]:
                            message_ids[match_key].pop(chat_id, None)
                    elif "too many requests" in str(e).lower():
                        retry_after = int(str(e).split('retry after ')[-1])
                        logger.warning(f"Достигнут лимит Telegram API. Ожидание {retry_after} секунд")
                        time.sleep(retry_after)
                    else:
                        logger.error(f"Ошибка Telegram API при отправке в чат {chat_id}: {str(e)}")
                except Exception as e:
                    logger.error(f"Не удалось отправить сообщение в чат {chat_id}: {str(e)}")
        else:
            # Обычные сообщения просто рассылаем
            for chat_id in list(active_chats.keys()):
                try:
                    rate_limiter.wait_if_needed()
                    bot.send_message(chat_id, message_text)
                    active_chats[chat_id] = datetime.now()  # Обновляем время активности
                except telebot.apihelper.ApiTelegramException as e:
                    if "chat not found" in str(e).lower():
                        logger.info(f"Чат {chat_id} недоступен, удаляем из активных")
                        active_chats.pop(chat_id, None)
                    elif "too many requests" in str(e).lower():
                        retry_after = int(str(e).split('retry after ')[-1])
                        logger.warning(f"Достигнут лимит Telegram API. Ожидание {retry_after} секунд")
                        time.sleep(retry_after)
                    else:
                        logger.error(f"Ошибка Telegram API при отправке в чат {chat_id}: {str(e)}")
                except Exception as e:
                    logger.error(f"Не удалось отправить сообщение в чат {chat_id}: {str(e)}")
    
    except Exception as e:
        logger.error(f"Ошибка при рассылке сообщений: {str(e)}", exc_info=True)

# ==============================================
# ОБРАБОТЧИКИ КОМАНД TELEGRAM
# ==============================================
bot = telebot.TeleBot(TEL_TOKEN)

@bot.message_handler(commands=['start'])
def send_welcome(message):
    """Обработчик команды /start"""
    global admin_chat_id
    
    try:
        # Добавляем чат в активные
        active_chats[message.chat.id] = datetime.now()
        
        # Проверяем, является ли пользователь администратором
        if message.chat.id == admin_chat_id:
            bot.send_message(message.chat.id, "👑 Вы администратор бота! Ваши сообщения будут видны всем пользователям.")
            logger.info(f"Администратор подключен: {message.chat.id}")
        else:
            bot.send_message(message.chat.id, "🤖 Вы подключены к боту! Теперь вы будете получать все прогнозы!")
            logger.info(f"Новый пользователь подключен: {message.chat.id}")
        
        # Запускаем процессы, если это первый пользователь
        if len(active_chats) == 1:
            # Инициализируем базу данных если включен сбор статистики
            if STAT_DB:
                StatisticsDB.initialize_database()
            
            Thread(target=analyze_loop, daemon=True).start()
            Thread(target=results_check_loop, daemon=True).start()
            logger.info("Запущены фоновые процессы анализа матчей")
    
    except Exception as e:
        logger.error(f"Ошибка при обработке команды /start: {str(e)}", exc_info=True)

@bot.message_handler(commands=['info'])
def send_info(message):
    """Обработчик команды /info"""
    try:
        bot.send_message(message.chat.id, "🟢 Статус: Бот сканирует Матчи!")
        logger.info(f"Пользователь {message.chat.id} запросил информацию о статусе бота")
    
    except Exception as e:
        logger.error(f"Ошибка при обработке команды /info: {str(e)}", exc_info=True)

@bot.message_handler(func=lambda message: True)
def handle_message(message):
    """Обработчик текстовых сообщений"""
    global admin_chat_id
    
    try:
        # Сообщения администратора рассылаются всем
        if message.chat.id == admin_chat_id:
            broadcast_message(message.text)
            logger.info(f"Администратор отправил сообщение всем: {message.text}")
        else:
            bot.reply_to(message, "✉️ Ваше сообщение получено. Только администратор может отправлять сообщения всем.")
            logger.info(f"Получено сообщение от пользователя {message.chat.id}: {message.text}")
    
    except Exception as e:
        logger.error(f"Ошибка при обработке сообщения: {str(e)}", exc_info=True)

# ==============================================
# ФУНКЦИИ ЦИКЛОВ АНАЛИЗА
# ==============================================
def analyze_loop():
    """Бесконечный цикл анализа матчей"""
    logger.info("Запуск цикла анализа матчей...")
    
    while True:
        try:
            if active_chats:  # Работаем только если есть активные пользователи
                matches = analyze_matches()
                
                for match in matches:
                    # Проверяем, не был ли матч уже отправлен
                    if not any(m['key'] == match['key'] for m in sent_matches):
                        # Форматируем коэффициенты с двумя знаками после запятой
                        odds_formatted = f"{match['odds']:.2f}"
                        our_odds_formatted = f"{match['our_expected_odds']:.2f}"
                        
                        # Формируем сообщение для нового матча
                        message_text = (
                            f"🟢 Внимание! Новый прогноз!\n\n"
                            f"🏆 {match['league']}\n\n"
                            f"⚔️ {match['teams']}\n\n"
                            f"-----------------------------------------------\n"
                            f"📅 {match['date_str']} {match['time_str']}\n"
                            f"-----------------------------------------------\n"
                            f"{match['methods_visualization']}\n"
                            f"-----------------------------------------------\n"
                            f"📊 Вероятность:  {match['avg_probability']} %\n"
                            f"-----------------------------------------------\n"
                            f"📊 Наш Коэффициент:  {our_odds_formatted}\n"
                            f"-----------------------------------------------\n\n"
                            f"🔥 Прогноз: ТБ 2.5 за {odds_formatted}"
                        )
                        
                        # Рассылаем сообщение
                        broadcast_message(message_text, match['key'])
                        
                        # Добавляем матч в список отправленных
                        sent_matches.append(match)
                        
                        logger.info(f"Найден подходящий матч: {match['teams']} (вероятность: {match['avg_probability']}%)")
            
            # Ожидание перед следующим анализом
            time.sleep(300)
            logger.info("Запускаем следующий цикл анализа матчей ...")
            
        except Exception as e:
            logger.error(f"Критическая ошибка в analyze_loop: {str(e)}", exc_info=True)
            time.sleep(API_RETRY_DELAY)

def results_check_loop():
    """Бесконечный цикл проверки результатов матчей"""
    logger.info("Запуск цикла проверки результатов...")
    
    while True:
        try:
            if active_chats:  # Работаем только если есть активные пользователи
                check_match_results()
            
            # Ожидание перед следующей проверкой
            time.sleep(300)
            
        except Exception as e:
            logger.error(f"Критическая ошибка в results_check_loop: {str(e)}", exc_info=True)
            time.sleep(API_RETRY_DELAY)

# ==============================================
# ЗАПУСК БОТА
# ==============================================
if __name__ == "__main__":
    logger.info("Запуск Telegram бота...")
    logger.info(f"Администратор установлен: {ID_ADMIN}")
    logger.info(f"Сбор статистики: {'ВКЛЮЧЕН' if STAT_DB else 'ВЫКЛЮЧЕН'}")
    logger.info(f"Диапазон коэффициентов БК: {MIN_BOOKMAKER_ODDS} - {MAX_BOOKMAKER_ODDS}")
    
    try:
        # Инициализируем базу данных если включен сбор статистики
        if STAT_DB:
            StatisticsDB.initialize_database()
        
        # Запускаем бота в отдельном потоке
        bot_thread = Thread(target=bot.infinity_polling, kwargs={'timeout': POLLING_TIMEOUT}, daemon=True)
        bot_thread.start()
        
        logger.info("Бот успешно запущен и ожидает подключений...")
        
        # Основной поток остается активным
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("Бот остановлен пользователем")
    except Exception as e:
        logger.critical(f"Критическая ошибка при запуске бота: {str(e)}", exc_info=True)