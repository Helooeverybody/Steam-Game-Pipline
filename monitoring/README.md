# Grafana Guidance

This guide helps access and use Grafana for the Steam Analytics dashboards.

## Prerequisites

- Grafana,Mongodb and Prometheus deployments running in the cluster

## Connection Configuration

### 1. Access Grafana

```wsl
# Port forward Grafana if not exposed
k3s kubectl port-forward svc/grafana 3000:3000 -n monitoring
```

Open http://localhost:3000 in your browser, you will see Grafana UI.

### 2. Add Datasource
1. **Prometheus**: has been configured and added by default
2. **MongoDB**:
- Go to **Configuration** → **Data Sources** → **Add data source**
- Select the **MongoDB** plugin
- Configure Connection
All settings has been filled by default. Choose **Authentication Method**: UserName/Password and click **Save & Test** to verify the connection.


| Setting | Value |
|---------|-------|
| **URL** | `mongodb://mongodb.default.svc.cluster.local:27017` |
| **Database** | `steam_analytics` |

---

## Dashboards

| Dashboards | DataSource | Description |
|------------|----------|--------------|
| `MongoDB Metrics` | Prometheus | Monitor MongoDB health with different metrics of connection and operation |
| `Steam Lens` | MongoDB | Show a lot of insights about Steam Games such as popular genres, most played games, game releases trend, etc |
| `Game Inspect` | MongoDB | Real-time stats of player count and reviews of games |
| `Dev Inspect` | MongoDB | Choose a developer to get insights about their games (total games, average price, most popular games, etc...) |
---

## Sample MongoDB Queries in Grafana

### Query 1: Games Released and Developers Active Each Year
**Collection:** release_trend
```json
[
  {
    "$project": {
      "_id": 0,
      "Year": "$release_year",
      "Games Released": "$total_games",
      "Active Developers": { "$size": { "$ifNull": ["$developers_active", []] } }
    }
  },
  { "$sort": { "Year": 1 } }
]
```

**Panel Type:** Trend

---

### Query 2: Most Hated Games 
**Collection:** game_fact
```json
[
  {
    "$group": {
      "_id": "$name",
      "pos": { "$max": "$positive_reviews" },
      "neg": { "$max": "$negative_reviews" }
    }
  },
  {
    "$addFields": {
      "total_feedback": { "$add": ["$pos", "$neg"] }
    }
  },
  {
    "$match": {
      "total_feedback": { "$gte": 10000 }
    }
  },
  {
    "$project": {
      "_id": 0,
      "game": "$_id",
      "Disapproval Rate": { "$divide": ["$neg", "$total_feedback"] }
    }
  },
  { "$sort": { "Disapproval Rate": -1 } },
  { "$limit": 10 }
]
```

**Panel Type:** Bar chart 

---

### Query 3: Genres Popularity
**Collection:** game_genre
```json
[
  {
    "$match": {
      "genre": { "$ne": "", "$exists": true }
    }
  },
  {
    "$project": {
      "_id": 0,
      "Genre": "$genre",
      "Game Count": "$total_games"
    }
  },
  { "$sort": { "Game Count": -1 } },
  { "$limit": 10 }
]
```

**Panel Type:** Bar Chart

---

## Quick Commands Reference

```wsl
# Port forward mongo-express to access MongoDB UI
k3s kubectl port-forward svc/mongo-express 8081:8081 -n database 

# Port forward Prometheus 
k3s kubectl port-forward svc/prometheus 9090:9090 -n monitoring

```

