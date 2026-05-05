### Pasul 1: Reset complet al infrastructurii
Rulează aceste comenzi în folderul unde ai `docker-compose.yml`:

```powershell
# 1. Oprim containerele și ȘTERGEM volumele (datele vechi care pot fi corupte)
docker-compose down -v

# 2. Pornim din nou
docker-compose up -d
```

### Pasul 2: Verificare (Așteaptă 15 secunde)
Kafka are nevoie de câteva secunde să se inițializeze. Așteaptă puțin și apoi verifică:

```powershell
docker ps
```
**Dacă la STATUS scrie `Up 15 seconds` (sau mai mult), e perfect.**

### Pasul 3: Dacă tot moare, verifică log-urile
Dacă tot nu rămâne pornit, trebuie să vedem exact de ce „moare”. Rulează:
```powershell
docker logs kafka
```
*(Dacă vezi erori de tipul "Port 9092 already in use", înseamnă că mai ai un alt Kafka pornit pe calculator în afara Docker-ului).*

---

### Pasul 4: Testarea Generatorului Go
Odată ce `docker ps` îți arată containerele pornite stabil (nu mai mor după o secundă), poți rula generatorul tău:

1. **Creează topicul:**
```powershell
docker exec -it kafka kafka-topics --create --topic iot-sensor-data --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

2. **Pornește generatorul (în folderul `generator`):**
```powershell
go run main.go
```

3. **Verifică datele (într-un alt terminal):**
```powershell
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic iot-sensor-data --from-beginning
```

**De ce am folosit `down -v`?**
Containerele tale aveau o vechime de 7 zile. Kafka stochează ID-ul de cluster în fișiere temporare. Dacă ai restartat Docker sau Windows între timp, acele ID-uri pot intra în conflict, iar Kafka refuză să pornească până nu ștergi datele vechi (ceea ce face `-v`).

**Te rog să-mi spui dacă după `docker-compose down -v` și `up -d`, containerele rămân pornite!**