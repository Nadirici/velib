

    /* ========== Constantes visuelles ========== */
    const RAMPS = {
      bikes: ["#86b6ef", "#5598e7", "#2a78d6", "#184f95"],
      docks: ["#7cc47c", "#4aae4a", "#189318", "#005e00"],
    };
    const CRITIC = "#d03b3b";
    const CLOSED = "#898781";

    /* ========== Carte Leaflet ========== */
    // Renderer canvas partagÃ©, padding large : le canvas dÃ©borde du viewport,
    // donc un petit pan ne provoque pas de redraw. Tous les marqueurs y sont
    // dessinÃ©s d'un coup (au lieu d'un layer SVG par station).
    const canvasRenderer = L.canvas({ padding: 0.5 });
    const map = L.map("map", { preferCanvas: true, renderer: canvasRenderer })
      .setView([48.859, 2.347], 12);
    const dark = matchMedia("(prefers-color-scheme: dark)");
    let tiles = null;

    function setTiles() {
      if (tiles) map.removeLayer(tiles);
      const style = dark.matches ? "dark_all" : "light_all";
      tiles = L.tileLayer(
        `https://{s}.basemaps.cartocdn.com/${style}/{z}/{x}/{y}{r}.png`,
        { attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> &copy; <a href="https://carto.com/">CARTO</a>', maxZoom: 19 }
      ).addTo(map);
    }
    setTiles();
    if (dark.addEventListener) dark.addEventListener("change", setTiles);
    else if (dark.addListener) dark.addListener(setTiles);

    // Sous CLUSTER_ZOOM, on masque les ~1516 stations et on affiche des bulles
    // agrÃ©gÃ©es : par arrondissement dans Paris, par proximitÃ© en banlieue.
    // Plus lisible au dÃ©zoom, et beaucoup plus lÃ©ger (les marqueurs individuels
    // ne sont plus rendus).
    const CLUSTER_ZOOM = 13;
    const stationLayer = L.layerGroup().addTo(map);
    const clusterLayer = L.layerGroup();

    /* ========== Ã‰tat global ========== */
    let mode = "bikes";
    const markers = new Map();
    let stations = new Map();
    let lastTs = null, clockOffset = 0;
    let connected = false;

    /* ========== Encodage visuel ========== */
    function isClosed(s) {
      return !s.is_installed || (mode === "bikes" ? !s.is_renting : !s.is_returning);
    }
    function count(s) { return mode === "bikes" ? s.bikes_available : s.docks_available; }

    function color(s) {
      if (isClosed(s)) return CLOSED;
      const n = count(s);
      if (n === 0) return CRITIC;
      const ratio = s.capacity > 0 ? n / s.capacity : 1;
      const ramp = RAMPS[mode];
      return ramp[Math.min(3, Math.floor(ratio * 4))];
    }

    function radius(s) {
      return Math.max(5, Math.min(10, 4 + s.capacity / 12));
    }

    function style(s) {
      return {
        fillColor: color(s), fillOpacity: 0.85, radius: radius(s),
        color: dark.matches ? "#1a1a19" : "#ffffff", weight: 1.5,
      };
    }

    /* ========== Popup ========== */
    function fmtAge(sec) {
      if (sec < 60) return `${Math.max(0, Math.round(sec))} s`;
      if (sec < 3600) return `${Math.round(sec / 60)} min`;
      return `${Math.round(sec / 3600)} h`;
    }

    function popupHtml(id) {
      const s = stations.get(id);
      const now = Date.now() / 1000 + clockOffset;
      const pctBikes = s.capacity ? (100 * s.bikes_available / s.capacity) : 0;
      const status = !s.is_installed ? "â›” Hors service"
        : (!s.is_renting && !s.is_returning) ? "â›” FermÃ©e (ni location ni retour)"
        : !s.is_renting ? "âš ï¸ Location suspendue"
        : !s.is_returning ? "âš ï¸ Retour suspendu"
        : "âœ… En service";
      const delta = s.bikes_delta > 0 ? `+${s.bikes_delta}` : `${s.bikes_delta || 0}`;
      return `<div class="popup">
    <h3>${s.name}</h3>
    <div class="code">nÂ° ${s.station_code}</div>
    <table>
      <tr><td>VÃ©los disponibles</td><td><b>${s.bikes_available}</b></td></tr>
      <tr><td>&nbsp;&nbsp;dont mÃ©caniques</td><td>${s.mechanical}</td></tr>
      <tr><td>&nbsp;&nbsp;dont Ã©lectriques</td><td>${s.ebike}</td></tr>
      <tr><td>Bornettes libres</td><td><b>${s.docks_available}</b></td></tr>
      <tr><td>CapacitÃ©</td><td>${s.capacity}</td></tr>
    </table>
    <div class="bar"><i style="width:${pctBikes}%;background:${RAMPS.bikes[2]}"></i></div>
    <div class="status">${status}</div>
    <div class="delta">Dernier changement : ${delta} vÃ©lo(s) Â· signalÃ© il y a ${fmtAge(now - (s.last_reported || s.ts))}</div>
  </div>`;
    }

    /* ========== Rendu des marqueurs ========== */
    // On coupe les ondes radar pendant un dÃ©placement de carte et au-delÃ  d'un
    // plafond de pings simultanÃ©s : c'est ce qui gardait le pan fluide mÃªme en
    // pleine rafale d'Ã©vÃ©nements.
    let mapMoving = false, activePings = 0;
    function ping(m, s) {
      if (mapMoving || activePings > 36 || map.getZoom() < CLUSTER_ZOOM) return;
      activePings++;
      const base = radius(s);
      const ghost = L.circleMarker(m.getLatLng(), {
        radius: base, fill: false, color: color(s), weight: 2, opacity: 0.8,
      }).addTo(map);
      const t0 = performance.now();
      (function step(t) {
        const k = (t - t0) / 900;
        if (k >= 1) { map.removeLayer(ghost); activePings--; return; }
        ghost.setStyle({ opacity: 0.8 * (1 - k) });
        ghost.setRadius(base + 22 * k);
        requestAnimationFrame(step);
      })(t0);
    }
    map.on("movestart zoomstart", function () { mapMoving = true; });
    map.on("moveend zoomend", function () {
      mapMoving = false;
      if (!pendingRestyle.size) return;
      for (const id of pendingRestyle) {
        const m = markers.get(id), s = stations.get(id);
        if (m && s) restyleMarker(m, s, id);
      }
      pendingRestyle.clear();
    });

    // File des marqueurs Ã  re-styler quand on relÃ¢chera la carte : pendant un
    // pan/zoom on n'appelle JAMAIS setStyle (donc aucun redraw du canvas au
    // milieu du geste) ; on rejoue tout Ã  moveend.
    const pendingRestyle = new Set();

    function restyleMarker(m, s, id) {
      const c = color(s);
      if (c !== m._fill) {          // redraw uniquement si la COULEUR a changÃ©
        m.setStyle(style(s));
        m._fill = c;
      }
      if (m.isPopupOpen()) m.setPopupContent(popupHtml(id));
    }

    function upsertMarker(id, s) {
      let m = markers.get(id);
      if (!m) {
        m = L.circleMarker([s.lat, s.lon], style(s)).addTo(stationLayer);
        m.bindPopup(() => popupHtml(id), { maxWidth: 280 });
        m._fill = color(s);
        markers.set(id, m);
        return m;
      }
      if (mapMoving) { pendingRestyle.add(id); return m; }
      restyleMarker(m, s, id);
      return m;
    }

    function restyleAll() {
      for (const [id, s] of stations) upsertMarker(id, s);
    }

    /* ========== Clustering au dÃ©zoom ========== */
    // Couleur d'une bulle d'aprÃ¨s son taux de remplissage, dans la rampe du mode.
    function clusterColor(ratio, val) {
      if (val === 0) return CRITIC;
      return RAMPS[mode][Math.min(3, Math.floor(ratio * 4))];
    }

    function clusterBubble(lat, lon, label, size, bg, tip, banlieue) {
      const icon = L.divIcon({
        className: "",
        html: `<div class="cluster-bubble${banlieue ? " banlieue" : ""}" ` +
          `style="width:${size}px;height:${size}px;background:${bg};` +
          `font-size:${size > 34 ? 13 : 11}px">${label}</div>`,
        iconSize: [size, size],
        iconAnchor: [size / 2, size / 2],
      });
      const m = L.marker([lat, lon], { icon });
      m.bindTooltip(tip);
      m.on("click", () => map.flyTo([lat, lon], CLUSTER_ZOOM + 1, { duration: 0.6 }));
      return m;
    }

    function buildClusters() {
      clusterLayer.clearLayers();
      const paris = new Map(), banlieue = new Map();
      for (const s of stations.values()) {
        const a = arrOf(s);
        const bucket = a != null ? paris : banlieue;
        // Paris : regroupÃ© par arrondissement ; banlieue : par cellule ~3 km.
        const key = a != null ? a : Math.round(s.lat / 0.03) + "_" + Math.round(s.lon / 0.03);
        let o = bucket.get(key);
        if (!o) { o = { arr: a, n: 0, bikes: 0, docks: 0, cap: 0, lat: 0, lon: 0 }; bucket.set(key, o); }
        o.n++;
        o.bikes += s.bikes_available || 0;
        o.docks += s.docks_available || 0;
        o.cap += s.capacity || 0;
        o.lat += s.lat;
        o.lon += s.lon;
      }
      for (const o of paris.values()) {
        const val = mode === "bikes" ? o.bikes : o.docks;
        const ratio = o.cap ? val / o.cap : 0;
        const size = Math.round(24 + Math.min(o.n, 90) / 90 * 22);
        clusterLayer.addLayer(clusterBubble(o.lat / o.n, o.lon / o.n, o.arr, size,
          clusterColor(ratio, val),
          `<b>${o.arr}áµ‰ arrondissement</b><br>${o.n} stations Â· ${fmtN(o.bikes)} vÃ©los Â· ${fmtN(o.docks)} bornettes`));
      }
      for (const o of banlieue.values()) {
        const size = Math.round(20 + Math.min(o.n, 50) / 50 * 18);
        clusterLayer.addLayer(clusterBubble(o.lat / o.n, o.lon / o.n, o.n, size,
          CLOSED, `${o.n} stations Â· ${fmtN(o.bikes)} vÃ©los`, true));
      }
    }

    function updateMapMode() {
      const clustered = map.getZoom() < CLUSTER_ZOOM;
      if (clustered) {
        if (map.hasLayer(stationLayer)) map.removeLayer(stationLayer);
        buildClusters();
        if (!map.hasLayer(clusterLayer)) map.addLayer(clusterLayer);
      } else {
        if (map.hasLayer(clusterLayer)) map.removeLayer(clusterLayer);
        if (!map.hasLayer(stationLayer)) map.addLayer(stationLayer);
      }
    }
    map.on("zoomend", updateMapMode);
    // RafraÃ®chit les couleurs des clusters quand la carte est visible et dÃ©zoomÃ©e.
    setInterval(function () {
      if (!panelOpen && map.getZoom() < CLUSTER_ZOOM) buildClusters();
    }, 5000);

    /* ========== KPIs du bandeau ========== */
    function renderStats() {
      let bikes = 0, mech = 0, ebike = 0, docks = 0, empty = 0, full = 0, closed = 0;
      for (const s of stations.values()) {
        bikes += (s.bikes_available || 0);
        mech += (s.mechanical || 0);
        ebike += (s.ebike || 0);
        docks += (s.docks_available || 0);
        const open = s.is_installed && s.is_renting;
        if (!open) closed++;
        else if (s.bikes_available === 0) empty++;
        if (s.is_installed && s.is_returning && s.docks_available === 0) full++;
      }
      const fmt = (n) => n.toLocaleString("fr-FR");
      document.getElementById("stBikes").textContent = fmt(bikes);
      document.getElementById("stBikesSub").textContent = `dont ${fmt(mech)} mÃ©ca Â· ${fmt(ebike)} Ã©lec`;
      document.getElementById("stDocks").textContent = fmt(docks);
      document.getElementById("stEmpty").textContent = fmt(empty);
      document.getElementById("stFull").textContent = fmt(full);
      document.getElementById("stClosed").textContent = fmt(closed);
    }

    // Rafale d'Ã©vÃ©nements (un tour de producer) : au lieu de relancer le
    // travail O(n) par Ã©vÃ©nement, on marque Â« Ã  rafraÃ®chir Â» et on flushe une
    // seule fois Ã  la prochaine frame â€” le pan reste fluide sous la charge.
    let statsDirty = false;
    function scheduleStats() {
      if (statsDirty) return;
      statsDirty = true;
      requestAnimationFrame(function () {
        statsDirty = false;
        renderStats();
        renderLive();
      });
    }

    /* ========== LÃ©gende ========== */
    const legend = L.control({ position: "bottomleft" });
    legend.onAdd = () => {
      const div = L.DomUtil.create("div", "legend");
      legend._div = div;
      renderLegend();
      return div;
    };

    function renderLegend() {
      if (!legend._div) return;
      const ramp = RAMPS[mode];
      const noun = mode === "bikes" ? "vÃ©los" : "bornettes";
      const sw = (c) => `<span class="swatch" style="background:${c}"></span>`;
      legend._div.innerHTML = `
    <div class="title">Remplissage (${noun})</div>
    ${sw(CRITIC)}aucun ${mode === "bikes" ? "vÃ©lo" : "bornette libre"}<br>
    ${sw(ramp[0])}jusqu'Ã  25 %<br>
    ${sw(ramp[1])}25 â€“ 50 %<br>
    ${sw(ramp[2])}50 â€“ 75 %<br>
    ${sw(ramp[3])}plus de 75 %<br>
    ${sw(CLOSED)}station fermÃ©e`;
    }
    legend.addTo(map);

    /* ========== Badge de direct & fraÃ®cheur ========== */
    function renderLive() {
      const badge = document.getElementById("liveBadge");
      const label = document.getElementById("liveLabel");
      const text = document.getElementById("freshText");
      if (!connected) {
        badge.className = "live off";
        label.textContent = "CONNEXIONâ€¦";
        text.textContent = "";
        return;
      }
      const age = lastTs == null ? null : Date.now() / 1000 + clockOffset - lastTs;
      const stale = age != null && age > 180;
      badge.className = stale ? "live stale" : "live";
      label.textContent = stale ? "FLUX INTERROMPU" : "EN DIRECT";
      text.textContent = age == null ? "" : `Â· dernier Ã©vÃ©nement il y a ${fmtAge(age)}`;
    }
    setInterval(renderLive, 5000);

    /* ========== Ã‰tage 1 : snapshot (Redis) ========== */
    async function loadSnapshot() {
      const payload = await (await fetch("/api/stations")).json();
      clockOffset = payload.now - Date.now() / 1000;
      lastTs = payload.last_ts;
      stations = new Map(payload.stations.map((s) => [s.station_id, s]));
      restyleAll();
      updateMapMode();      // clusters au dÃ©zoom, stations individuelles au zoom
      renderStats();
      renderLive();
    }

    /* ========== Ã‰tage 2 : flux SSE ========== */
    let everConnected = false;

    function applyEvent(s) {
      const old = stations.get(s.station_id);
      if (old) {
        s.taken = old.taken || 0;
        s.returned = old.returned || 0;
      } else {
        s.taken = 0;
        s.returned = 0;
      }
      if (s.bikes_delta < 0) s.taken += -s.bikes_delta;
      else if (s.bikes_delta > 0) s.returned += s.bikes_delta;

      stations.set(s.station_id, s);
      lastTs = s.ts;
      const m = upsertMarker(s.station_id, s);
      ping(m, s);
      bumpActivity(s);
      scheduleStats();
    }

    function connectStream() {
      const es = new EventSource("/api/stream");
      es.onopen = async () => {
        connected = true;
        if (everConnected) await loadSnapshot();
        everConnected = true;
        renderLive();
      };
      es.onerror = () => {
        connected = false;
        renderLive();
      };
      es.onmessage = (e) => applyEvent(JSON.parse(e.data));
    }

    loadSnapshot().then(connectStream);

    /* ================== Panneau BI ================== */
    let panelOpen = false, biTab = "kpi", step = 300;
    let actPoints = new Map();
    let dayStart = null;
    let kpi = { events: 0, taken: 0, returned: 0 };

    const cssVar = (n) => getComputedStyle(document.documentElement).getPropertyValue(n).trim();
    const fmtN = (n) => n.toLocaleString("fr-FR");
    const hhmm = (t) => new Date(t * 1000).toLocaleTimeString("fr-FR", { hour: "2-digit", minute: "2-digit" });

    async function loadActivity() {
      const r = await (await fetch(`/api/activity?step=${step}`)).json();
      dayStart = r.day_start;
      actPoints = new Map(r.points.map((p) => [p.t, p]));
      kpi = { events: 0, taken: 0, returned: 0 };
      for (const p of r.points) {
        kpi.events += p.events;
        kpi.taken += p.taken;
        kpi.returned += p.returned;
      }
      renderKpis();
      renderTop(r.top);
      renderLiveChart();
    }

    function renderKpis() {
      document.getElementById("kEvents").textContent = fmtN(kpi.events);
      document.getElementById("kTaken").textContent = fmtN(kpi.taken);
      document.getElementById("kReturned").textContent = fmtN(kpi.returned);
    }

    function renderTop(top) {
      document.getElementById("topList").innerHTML = top.length
        ? top.map((s) => `<div class="row"><span>${s.name}</span><b>${fmtN(s.moves)}</b></div>`).join("")
        : `<div class="empty-note">Pas encore de mouvement aujourd'hui.</div>`;
    }

    function bumpActivity(s) {
      if (dayStart == null) return;
      const t = Math.floor(s.ts / step) * step;
      const p = actPoints.get(t) || { t: t, events: 0, taken: 0, returned: 0 };
      p.events++; kpi.events++;
      if (s.bikes_delta < 0) { p.taken += -s.bikes_delta; kpi.taken += -s.bikes_delta; }
      else if (s.bikes_delta > 0) { p.returned += s.bikes_delta; kpi.returned += s.bikes_delta; }
      actPoints.set(t, p);
      // Tranches minute des KPIs : entretenues en parallÃ¨le, quelle que soit
      // la granularitÃ© choisie dans l'onglet Â« direct Â».
      const tm = Math.floor(s.ts / 60) * 60;
      const pm = minutePoints.get(tm) || { t: tm, events: 0, taken: 0, returned: 0 };
      pm.events++;
      if (s.bikes_delta < 0) pm.taken += -s.bikes_delta;
      else if (s.bikes_delta > 0) pm.returned += s.bikes_delta;
      minutePoints.set(tm, pm);
      if (panelOpen && biTab === "live") { renderKpis(); scheduleLiveChart(); }
      if (panelOpen && biTab === "kpi") scheduleKpis();
      if (panelOpen && biTab === "biz") scheduleBiz();
    }

    let liveChartTimer = null;
    function scheduleLiveChart() {
      if (liveChartTimer) return;
      liveChartTimer = setTimeout(function () { liveChartTimer = null; renderLiveChart(); }, 400);
    }

    /* ================== Onglet MÃ©tier (vue exploitant) ================== */
    let bizData = null, bizProfile = null, bizPrice = 1.00, bizTimer = null;

    function scheduleBiz() {
      // Les Ã©vÃ©nements arrivent par vagues (~1/min) : on regroupe le refetch.
      if (bizTimer) return;
      bizTimer = setTimeout(function () { bizTimer = null; loadBizTab(); }, 3000);
    }

    async function loadBizTab() {
      if (bizProfile == null) {
        try {
          const p = await (await fetch("/api/history/profile")).json();
          bizProfile = (p.hours && p.hours.length) ? p.hours : false;
        } catch (err) { bizProfile = false; }
      }
      
      let today_taken = 0, today_returned = 0;
      let at_risk = 0;
      const nets = [];
      const rebalance = [];
      
      for (const s of stations.values()) {
        const taken = s.taken || 0;
        const returned = s.returned || 0;
        const moves = taken + returned;
        const net = returned - taken;
        
        today_taken += taken;
        today_returned += returned;
        
        if (net !== 0) {
          nets.push({ station_id: s.station_id, name: s.name, net: net, lat: s.lat, lon: s.lon });
        }
        
        const out_of_order = !s.is_installed || !(s.is_renting || s.is_returning);
        const empty = s.is_installed && s.is_renting && s.bikes_available === 0;
        const full = s.is_installed && s.is_returning && s.docks_available === 0;
        
        if (out_of_order || empty || full) at_risk += moves;
        
        if (empty || full) {
          rebalance.push({
            station_id: s.station_id, name: s.name, lat: s.lat, lon: s.lon,
            state: empty ? "vide" : "pleine", moves: moves,
            capacity: s.capacity, bikes: s.bikes_available, docks: s.docks_available
          });
        }
      }
      
      rebalance.sort((a, b) => b.moves - a.moves);
      nets.sort((a, b) => b.net - a.net);
      
      bizData = {
        today_taken: today_taken,
        today_returned: today_returned,
        total_moves: today_taken + today_returned,
        at_risk_moves: at_risk,
        to_move: Math.floor(nets.reduce((sum, n) => sum + Math.abs(n.net), 0) / 2),
        rebalance: rebalance.slice(0, 12),
        sinks: nets.filter(n => n.net > 0).slice(0, 8),
        sources: [...nets].filter(n => n.net < 0).reverse().slice(0, 8)
      };
      
      renderBizTab();
    }

    function renderBizTab() {
      if (biTab !== "biz" || !bizData) return;
      const d = bizData;

      document.getElementById("bzTrips").textContent = fmtN(d.today_taken);
      document.getElementById("bzTripsSub").textContent =
        fmtN(d.today_returned) + " vÃ©los rendus en face";
      document.getElementById("bzRevenue").textContent =
        fmtN(Math.round(d.today_taken * bizPrice)) + " â‚¬";

      let fleet = 0;
      for (const s of stations.values()) fleet += s.bikes_available || 0;
      document.getElementById("bzPerBike").textContent =
        fleet ? (d.today_taken / fleet).toFixed(2).replace(".", ",") : "â€“";

      /* Projection fin de journÃ©e : la part de la demande quotidienne dÃ©jÃ 
         Ã©coulÃ©e Ã  cette heure-ci, d'aprÃ¨s le profil horaire moyen archivÃ©. */
      const projEl = document.getElementById("bzProj");
      const projSub = document.getElementById("bzProjSub");
      if (bizProfile && bizProfile.length) {
        const nowH = new Date().getHours() + new Date().getMinutes() / 60;
        let done = 0, totalDay = 0;
        for (const h of bizProfile) {
          totalDay += h.taken;
          if (h.hour + 1 <= nowH) done += h.taken;
          else if (h.hour < nowH) done += h.taken * (nowH - h.hour);
        }
        const share = totalDay ? done / totalDay : 0;
        if (share > 0.05) {
          projEl.textContent = "â‰ˆ " + fmtN(Math.round(d.today_taken / share)) + " courses";
          projSub.textContent = pct(share) + " de la journÃ©e type dÃ©jÃ  Ã©coulÃ©e";
        } else {
          projEl.textContent = "â€“";
          projSub.textContent = "trop tÃ´t pour projeter";
        }
      } else {
        projEl.textContent = "â€“";
        projSub.textContent = "nÃ©cessite des archives (lance l'archiveur)";
      }

      document.getElementById("bzToMove").textContent = fmtN(d.to_move);
      document.getElementById("bzRisk").textContent =
        d.total_moves ? pct(d.at_risk_moves / d.total_moves) : "â€“";
      document.getElementById("bzRiskSub").textContent =
        fmtN(d.rebalance.length) + " station(s) en dÃ©faut listÃ©e(s)";

      const tbody = document.getElementById("bzRebalance");
      document.getElementById("bzRebalanceEmpty").style.display =
        d.rebalance.length ? "none" : "block";
      tbody.innerHTML = d.rebalance.map(function (s) {
        return '<tr data-lat="' + s.lat + '" data-lon="' + s.lon + '" data-id="' + s.station_id + '">' +
          '<td>' + s.name + '</td>' +
          '<td><span class="bstate ' + s.state + '">' + s.state.toUpperCase() + '</span></td>' +
          '<td class="num">' + fmtN(s.moves) + '</td>' +
          '<td class="num">' + s.bikes + '</td>' +
          '<td class="num">' + s.docks + '</td></tr>';
      }).join("");

      const flowRow = function (s) {
        const cls = s.net > 0 ? "net-pos" : "net-neg";
        const sign = s.net > 0 ? "+" : "";
        return '<div class="row"><span>' + s.name + '</span><b class="' + cls + '">' +
          sign + fmtN(s.net) + '</b></div>';
      };
      document.getElementById("bzSinks").innerHTML =
        d.sinks.length ? d.sinks.map(flowRow).join("") : '<div class="empty-note">â€”</div>';
      document.getElementById("bzSources").innerHTML =
        d.sources.length ? d.sources.map(flowRow).join("") : '<div class="empty-note">â€”</div>';
    }

    document.getElementById("bzPrice").addEventListener("input", function (e) {
      bizPrice = Number(e.target.value) || 0;
      renderBizTab();
    });

    /* Clic sur une ligne de rÃ©Ã©quilibrage â†’ zoom carte + popup */
    document.getElementById("bzRebalance").addEventListener("click", function (e) {
      const tr = e.target.closest("tr");
      if (!tr) return;
      map.flyTo([Number(tr.dataset.lat), Number(tr.dataset.lon)], 16, { duration: 0.8 });
      const mk = markers.get(Number(tr.dataset.id));
      if (mk) mk.openPopup();
    });

    /* ================== Onglet Arrondissements ================== */
    // L'arrondissement se lit dans le prÃ©fixe du station_code (16107 â†’ 16) :
    // 1..20 = Paris intra-muros, >= 21 = banlieue.
    function arrOf(s) {
      const a = Math.floor(parseInt(s.station_code, 10) / 1000);
      return (a >= 1 && a <= 20) ? a : null;
    }

    let arrData = [], arrGeo = null, arrSelected = null;

    // Ã‰chelle de chaleur bleu (activitÃ© faible) â†’ rouge (forte).
    function heatColor(t) {
      t = Math.max(0, Math.min(1, t));
      const lerp = (a, b) => Math.round(a + (b - a) * t);
      return `rgb(${lerp(42, 208)},${lerp(120, 59)},${lerp(214, 59)})`;
    }

    async function loadArrTab() {
      if (!arrGeo) {
        try {
          arrGeo = await (await fetch("/arrondissements.geojson")).json();
        } catch (e) {
          console.error("Impossible de charger les arrondissements");
        }
      }
      
      const acc = new Map();
      for (const s of stations.values()) {
        const arrId = arrOf(s);
        if (arrId === null) continue;
        
        let a = acc.get(arrId);
        if (!a) {
          a = { arr: arrId, stations: 0, bikes: 0, ebike: 0, capacity: 0, empty: 0, full: 0, taken: 0, returned: 0, docks: 0, lat: 0, lon: 0 };
          acc.set(arrId, a);
        }
        
        a.stations++;
        a.bikes += s.bikes_available || 0;
        a.ebike += s.ebike || 0;
        a.capacity += s.capacity || 0;
        a.docks += s.docks_available || 0;
        a.lat += s.lat;
        a.lon += s.lon;
        
        if (s.is_installed && s.is_renting && s.bikes_available === 0) a.empty++;
        if (s.is_installed && s.is_returning && s.docks_available === 0) a.full++;
        
        a.taken += s.taken || 0;
        a.returned += s.returned || 0;
      }
      
      arrData = Array.from(acc.values()).sort((a, b) => a.arr - b.arr);
      for (const a of arrData) {
        a.lat /= a.stations;
        a.lon /= a.stations;
      }
      
      renderArrChoro();
      renderArrDetail(arrSelected);
      renderArrTops();
    }

    function renderArrTops() {
      const row = (name, val) => `<div class="row"><span>${name}</span><b>${val}</b></div>`;
      
      let targetStations = Array.from(stations.values());
      if (arrSelected !== null) {
          targetStations = targetStations.filter(s => arrOf(s) === arrSelected);
      }
      if (!targetStations.length) {
          document.getElementById("arrTopAct").innerHTML = "";
          document.getElementById("arrTopFill").innerHTML = "";
          return;
      }
      
      const stSort = [...targetStations].sort((a, b) => ((b.taken||0) + (b.returned||0)) - ((a.taken||0) + (a.returned||0)));
      const topSt = stSort.slice(0, 5);
      const bottomSt = [...stSort].reverse().slice(0, 5);
      
      document.getElementById("arrTopAct").innerHTML = 
        `<div style="font-size: 11.5px; color: var(--muted); margin: 4px 0;">Les plus actives</div>` +
        topSt.map(s => row(s.name, fmtN((s.taken||0) + (s.returned||0)) + " mvts")).join('') +
        `<div style="font-size: 11.5px; color: var(--muted); margin: 12px 0 4px 0;">Les moins actives</div>` +
        bottomSt.map(s => row(s.name, fmtN((s.taken||0) + (s.returned||0)) + " mvts")).join('');
      
      const stFill = targetStations
         .filter(s => s.capacity > 0)
         .map(s => ({ name: s.name, fill: s.bikes_available / s.capacity }))
         .sort((a, b) => b.fill - a.fill);
         
      const topFull = stFill.slice(0, 3);
      const topEmpty = [...stFill].reverse().slice(0, 3);
      
      document.getElementById("arrTopFill").innerHTML = 
        `<div style="font-size: 11.5px; color: var(--muted); margin: 4px 0;">Les plus pleines</div>` +
        topFull.map(a => row(a.name, Math.round(a.fill * 100) + " %")).join('') +
        `<div style="font-size: 11.5px; color: var(--muted); margin: 12px 0 4px 0;">Les plus vides</div>` +
        topEmpty.map(a => row(a.name, Math.round(a.fill * 100) + " %")).join('');
    }

    let arrSvgBuilt = false;
    let arrSvgW = 0, arrSvgH = 0;

    function renderArrChoro() {
      const container = document.getElementById("arrChoro");
      if (!arrGeo || !arrData.length) return;
      
      const W = container.clientWidth || 600;
      const H = container.clientHeight || 500;
      
      if (!arrSvgBuilt || arrSvgW !== W || arrSvgH !== H) {
        arrSvgW = W;
        arrSvgH = H;
        
        let minLon = 999, maxLon = -999, minLat = 999, maxLat = -999;
        for (const f of arrGeo.features) {
          const coords = f.geometry.type === 'Polygon' ? f.geometry.coordinates : f.geometry.coordinates.flat();
          for (const ring of coords) {
            for (const [lon, lat] of ring) {
              if (lon < minLon) minLon = lon;
              if (lon > maxLon) maxLon = lon;
              if (lat < minLat) minLat = lat;
              if (lat > maxLat) maxLat = lat;
            }
          }
        }
        
        const margin = 0.005;
        minLon -= margin; maxLon += margin;
        minLat -= margin; maxLat += margin;
        
        const scaleX = W / (maxLon - minLon);
        const scaleY = H / (maxLat - minLat);
        const scale = Math.min(scaleX * 0.66, scaleY);
        
        const offsetX = (W - (maxLon - minLon) * scale / 0.66) / 2;
        const offsetY = (H - (maxLat - minLat) * scale) / 2;
        
        function project(lon, lat) {
          const x = offsetX + (lon - minLon) * scale / 0.66;
          const y = H - (offsetY + (lat - minLat) * scale);
          return `${x.toFixed(1)},${y.toFixed(1)}`;
        }

        let paths = '';
        for (const f of arrGeo.features) {
          const arrId = f.properties.arr;
          
          const drawPoly = (rings) => {
            return rings.map(ring => {
              const pts = ring.map(([lon, lat]) => project(lon, lat));
              return `M${pts.join(' L')} Z`;
            }).join(' ');
          };

          let d = '';
          if (f.geometry.type === 'Polygon') d = drawPoly(f.geometry.coordinates);
          else if (f.geometry.type === 'MultiPolygon') d = f.geometry.coordinates.map(drawPoly).join(' ');
          
          paths += `<path id="arr-path-${arrId}" d="${d}" style="cursor:pointer; transition: opacity 0.2s, stroke-width 0.2s" onclick="selectArr(${arrId})"><title>${arrId}áµ‰ arrondissement</title></path>`;
          
          const aData = arrData.find(a => a.arr === arrId);
          if (aData) {
             const cx = project(aData.lon, aData.lat).split(',');
             paths += `<text id="arr-text-${arrId}" x="${cx[0]}" y="${cx[1]}" text-anchor="middle" dominant-baseline="central" fill="white" style="font-size: 13px; font-weight: bold; pointer-events: none; text-shadow: 0px 0px 3px rgba(0,0,0,0.8)">${arrId}</text>`;
          }
        }
        container.innerHTML = `<svg width="${W}" height="${H}">${paths}</svg>`;
        arrSvgBuilt = true;
      }
      
      const acts = arrData.map((a) => a.taken + a.returned);
      const minAct = Math.min(...acts), maxAct = Math.max(...acts, minAct + 1);

      for (const a of arrData) {
        const arrId = a.arr;
        const path = document.getElementById(`arr-path-${arrId}`);
        const text = document.getElementById(`arr-text-${arrId}`);
        if (!path) continue;
        
        const t = (a.taken + a.returned - minAct) / (maxAct - minAct);
        const color = heatColor(t);
        
        const isSelected = (arrSelected === arrId);
        const strokeColor = isSelected ? 'var(--ink)' : 'var(--ink-2)';
        const strokeW = isSelected ? 2.5 : 1;
        const opacity = (arrSelected !== null && !isSelected) ? 0.4 : 1;
        
        path.setAttribute("fill", color);
        path.setAttribute("fill-opacity", opacity);
        path.setAttribute("stroke", strokeColor);
        path.setAttribute("stroke-width", strokeW);
        if (text) text.style.opacity = opacity;
      }
    }

    function selectArr(arr) {
      arrSelected = (arrSelected === arr) ? null : arr;
      renderArrChoro();
      renderArrDetail(arrSelected);
    }

    function renderArrDetail(arr) {
      const box = document.getElementById("arrDetail");
      const row = (label, val) => `<div class="drow"><span>${label}</span><b>${val}</b></div>`;
      if (arr == null) {
        let st = 0, bikes = 0, ebike = 0, cap = 0, empty = 0, taken = 0, returned = 0;
        for (const a of arrData) {
          st += a.stations; bikes += a.bikes; ebike += a.ebike; cap += a.capacity;
          empty += a.empty; taken += a.taken; returned += a.returned;
        }
        box.innerHTML =
          `<div class="dtitle">Paris intra-muros</div>` +
          `<div class="dsub">Vue d'ensemble â€” clic sur un arrondissement pour le dÃ©tail</div>` +
          row("Arrondissements", arrData.length) +
          row("Stations", fmtN(st)) +
          row("VÃ©los disponibles", fmtN(bikes)) +
          row("dont Ã©lectriques", fmtN(ebike)) +
          row("Remplissage moyen", cap ? Math.round(100 * bikes / cap) + " %" : "â€“") +
          row("Stations vides", fmtN(empty)) +
          row("ActivitÃ© du jour", fmtN(taken + returned) + " mvts") +
          row("VÃ©los pris / rendus", fmtN(taken) + " / " + fmtN(returned));
        return;
      }
      const a = arrData.find((x) => x.arr === arr);
      if (!a) return;
      const fill = a.capacity ? Math.round(100 * a.bikes / a.capacity) : 0;
      box.innerHTML =
        `<div class="dtitle">${a.arr}áµ‰ arrondissement</div>` +
        `<div class="dsub">${a.stations} stations</div>` +
        row("VÃ©los disponibles", fmtN(a.bikes)) +
        row("dont Ã©lectriques", fmtN(a.ebike)) +
        row("Bornettes libres", fmtN(a.docks)) +
        row("Remplissage", fill + " %") +
        row("Stations vides", a.empty) +
        row("Stations pleines", a.full) +
        row("ActivitÃ© du jour", fmtN(a.taken + a.returned) + " mvts") +
        row("VÃ©los pris", fmtN(a.taken)) +
        row("VÃ©los rendus", fmtN(a.returned));
    }

    /* ================== Onglet Indicateurs (KPIs) ================== */
    /* Deux familles : l'instantanÃ© (calculÃ© sur la photo `stations`, donc mis
       Ã  jour par chaque Ã©vÃ©nement SSE) et l'activitÃ© du jour (tranches d'une
       minute entretenues en parallÃ¨le de l'onglet direct). */
    let minutePoints = new Map();   // t (minute) -> {events, taken, returned}
    let kpiTop = [];
    let histDays = null;            // rÃ©sumÃ© des archives Parquet
    let kpiTimer = null;

    function scheduleKpis() {
      if (kpiTimer) return;
      kpiTimer = setTimeout(function () { kpiTimer = null; renderKpiTab(); }, 400);
    }

    async function loadKpiTab() {
      const r = await (await fetch("/api/activity?step=60")).json();
      dayStart = r.day_start;
      minutePoints = new Map(r.points.map((p) => [p.t, p]));
      kpiTop = r.top;
      try {
        const d = await (await fetch("/api/history/daily")).json();
        histDays = d.days && d.days.length ? d.days : null;
      } catch (err) { histDays = null; }
      renderKpiTab();
    }

    function pct(x) { return (100 * x).toFixed(1).replace(".", ",") + " %"; }

    function drawSpark(box, values) {
      const n = values.length;
      if (!n) { box.innerHTML = ""; return; }
      const W = Math.max(box.clientWidth || 0, 160), H = 34;
      const vmax = Math.max(5, ...values);
      const X = (i) => 2 + (i / (n - 1)) * (W - 8);
      const Y = (v) => H - 3 - (v / vmax) * (H - 8);
      const blue = cssVar("--series-blue");
      const pts = values.map((v, i) => X(i).toFixed(1) + "," + Y(v).toFixed(1)).join("L");
      box.innerHTML = '<svg viewBox="0 0 ' + W + ' ' + H + '" height="' + H + '">' +
        '<path d="M' + pts + '" fill="none" stroke="' + blue + '" stroke-width="1.5"/>' +
        '<circle cx="' + X(n - 1) + '" cy="' + Y(values[n - 1]) + '" r="2.5" fill="' + blue + '"/>' +
        '</svg>';
    }

    function renderKpiTab() {
      if (biTab !== "kpi") return;

      /* --- Ã‰tat instantanÃ© (photo stations) --- */
      let bikes = 0, mech = 0, ebike = 0, cap = 0, open = 0, total = 0,
        withBike = 0, empty = 0, full = 0;
      for (const s of stations.values()) {
        total++;
        if (!(s.is_installed && s.is_renting)) continue;
        open++;
        bikes += s.bikes_available || 0;
        mech += s.mechanical || 0;
        ebike += s.ebike || 0;
        cap += s.capacity || 0;
        if (s.bikes_available > 0) withBike++; else empty++;
        if (s.is_returning && s.docks_available === 0) full++;
      }
      const fill = cap ? bikes / cap : 0;
      document.getElementById("kpFill").textContent = pct(fill);
      document.getElementById("kpFillSub").textContent =
        fmtN(bikes) + " vÃ©los pour " + fmtN(cap) + " bornettes";
      document.getElementById("kpFillBar").style.width = (100 * fill) + "%";

      const elec = (mech + ebike) ? ebike / (mech + ebike) : 0;
      document.getElementById("kpElec").textContent = pct(elec);
      document.getElementById("kpElecSub").textContent =
        fmtN(ebike) + " Ã©lectriques Â· " + fmtN(mech) + " mÃ©caniques";
      document.getElementById("kpElecBarE").style.width = (100 * elec) + "%";
      document.getElementById("kpElecBarM").style.width = (100 * (1 - elec)) + "%";

      document.getElementById("kpAvail").textContent = open ? pct(withBike / open) : "â€“";
      document.getElementById("kpAvailSub").textContent =
        fmtN(empty) + " stations ouvertes sans vÃ©lo";
      document.getElementById("kpTension").textContent = fmtN(empty + full);
      document.getElementById("kpOpen").textContent = total ? pct(open / total) : "â€“";
      document.getElementById("kpOpenSub").textContent =
        fmtN(open) + " sur " + fmtN(total) + " stations";

      /* --- ActivitÃ© du jour (tranches minute) --- */
      let taken = 0, returned = 0;
      const hourly = new Map();
      for (const p of minutePoints.values()) {
        taken += p.taken; returned += p.returned;
        const h = Math.floor(p.t / 3600) * 3600;
        hourly.set(h, (hourly.get(h) || 0) + p.taken + p.returned);
      }
      document.getElementById("kpRot").textContent = fmtN(taken + returned);
      document.getElementById("kpRotSub").textContent =
        fmtN(taken) + " pris Â· " + fmtN(returned) + " rendus";
      const net = returned - taken;
      document.getElementById("kpNet").textContent = (net > 0 ? "+" : "") + fmtN(net);

      const now = Math.floor(Date.now() / 1000 + clockOffset);
      const nowMin = Math.floor(now / 60) * 60;
      let last15 = 0;
      const sparkVals = [];
      for (let t = nowMin - 59 * 60; t <= nowMin; t += 60) {
        const p = minutePoints.get(t);
        const v = p ? p.taken + p.returned : 0;
        sparkVals.push(v);
        if (t > nowMin - 15 * 60) last15 += v;
      }
      document.getElementById("kpRate").textContent =
        (last15 / 15).toFixed(1).replace(".", ",");
      drawSpark(document.getElementById("kpSpark"), sparkVals);

      let peakH = null, peakV = 0;
      for (const [h, v] of hourly) if (v > peakV) { peakV = v; peakH = h; }
      document.getElementById("kpPeak").textContent =
        peakH == null ? "â€“" : new Date(peakH * 1000).getHours() + " h";
      document.getElementById("kpPeakSub").textContent =
        peakH == null ? "" : fmtN(peakV) + " mouvements dans l'heure";

      if (kpiTop.length) {
        document.getElementById("kpTopSt").textContent = kpiTop[0].name;
        document.getElementById("kpTopStSub").textContent =
          fmtN(kpiTop[0].moves) + " mouvements aujourd'hui";
      }

      /* --- Perspective (archives Parquet) --- */
      if (histDays) {
        const avg = histDays.reduce((a, d) => a + d.taken + d.returned, 0) / histDays.length;
        document.getElementById("kpAvgRot").textContent = fmtN(Math.round(avg));
        document.getElementById("kpAvgRotSub").textContent =
          "sur " + histDays.length + " jour(s) archivÃ©(s)";
        document.getElementById("kpVsAvg").textContent = avg ? pct((taken + returned) / avg) : "â€“";
        document.getElementById("kpVsAvgSub").textContent =
          "d'une journÃ©e moyenne complÃ¨te";
      } else {
        document.getElementById("kpAvgRot").textContent = "â€“";
        document.getElementById("kpAvgRotSub").textContent =
          "aucune archive â€” lance l'archiveur ce soir";
        document.getElementById("kpVsAvg").textContent = "â€“";
        document.getElementById("kpVsAvgSub").textContent = "";
      }
    }

    /* ========== Utilitaires graphiques ========== */
    function niceCeil(v) {
      if (v <= 0) return 10;
      const p = Math.pow(10, Math.floor(Math.log10(v)));
      for (var i = 0; i < 4; i++) {
        var m = [1, 2, 5, 10][i];
        if (m * p >= v) return m * p;
      }
      return 10 * p;
    }

    // Glisser sur une courbe pour zoomer sur une plage de temps ; double-clic
    // ou bouton âŸ² pour rÃ©initialiser. La fenÃªtre de zoom est mÃ©morisÃ©e en
    // valeur X (temps) sur la box, donc elle survit aux redraws du direct.
    var chartDragging = false;

    function drawLineChart(box, xs, series, fmtX) {
      box._data = { xs: xs, series: series, fmtX: fmtX };
      paintLine(box);
    }

    function paintLine(box) {
      var d = box._data;
      if (!d || !d.xs.length) { box.innerHTML = ""; return; }
      var xs = d.xs, series = d.series, fmtX = d.fmtX, full = xs.length;

      // DÃ©coupe selon la fenÃªtre de zoom courante (en valeur X).
      var i0 = 0, i1 = full - 1;
      if (box._zx) {
        while (i0 < full - 1 && xs[i0] < box._zx.min) i0++;
        while (i1 > 0 && xs[i1] > box._zx.max) i1--;
        if (i1 - i0 < 1) { i0 = 0; i1 = full - 1; box._zx = null; }
      }
      var vx = xs.slice(i0, i1 + 1);
      var vs = series.map(function (s) { return { name: s.name, color: s.color, values: s.values.slice(i0, i1 + 1) }; });
      var n = vx.length;

      var W = Math.max(box.clientWidth || 0, 320), H = 170;
      var P = { l: 42, r: 14, t: 10, b: 22 };
      var iw = W - P.l - P.r, ih = H - P.t - P.b;
      var ymax = niceCeil(Math.max(5, ...vs.flatMap(function (s) { return s.values; })));
      var X = function (i) { return P.l + (n < 2 ? iw / 2 : (i / (n - 1)) * iw); };
      var Y = function (v) { return P.t + ih - (v / ymax) * ih; };
      var muted = cssVar("--muted"), gridc = cssVar("--border");

      var g = "";
      [0, 0.5, 1].forEach(function (f) {
        var yy = Y(ymax * f);
        g += '<line x1="' + P.l + '" y1="' + yy + '" x2="' + (W - P.r) + '" y2="' + yy + '" stroke="' + gridc + '"/>';
        g += '<text x="' + (P.l - 6) + '" y="' + (yy + 3) + '" text-anchor="end" font-size="10" fill="' + muted + '">' + fmtN(Math.round(ymax * f)) + '</text>';
      });
      var nt = Math.min(6, n);
      for (var k = 0; k < nt; k++) {
        var i = Math.round((k / Math.max(nt - 1, 1)) * (n - 1));
        g += '<text x="' + X(i) + '" y="' + (H - 6) + '" text-anchor="middle" font-size="10" fill="' + muted + '">' + fmtX(vx[i]) + '</text>';
      }
      var paths = vs.map(function (s) {
        return '<path d="M' + s.values.map(function (v, i) { return X(i).toFixed(1) + ',' + Y(v).toFixed(1); }).join("L") + '" fill="none" stroke="' + s.color + '" stroke-width="2" stroke-linejoin="round" stroke-linecap="round"/>';
      }).join("");

      var resetBtn = box._zx ? '<div class="chart-reset" title="RÃ©initialiser le zoom">âŸ²</div>' : '';
      box.innerHTML = '<svg viewBox="0 0 ' + W + ' ' + H + '" height="' + H + '">' + g + paths +
        '<line class="ch" x1="0" y1="' + P.t + '" x2="0" y2="' + (P.t + ih) + '" stroke="' + muted + '" visibility="hidden"/>' +
        '<rect class="plot" x="' + P.l + '" y="' + P.t + '" width="' + iw + '" height="' + ih + '" fill="transparent" style="cursor:crosshair"/>' +
        '</svg><div class="chart-sel"></div><div class="chart-tip"></div>' + resetBtn;

      var svg = box.querySelector("svg");
      var tip = box.querySelector(".chart-tip");
      var ch = box.querySelector(".ch");
      var sel = box.querySelector(".chart-sel");
      var plot = box.querySelector(".plot");

      function idxAt(clientX) {
        var r = svg.getBoundingClientRect();
        var mx = ((clientX - r.left) / r.width) * W;
        return Math.max(0, Math.min(n - 1, Math.round(((mx - P.l) / iw) * (n - 1))));
      }

      svg.addEventListener("mousemove", function (e) {
        if (chartDragging) return;
        var idx = idxAt(e.clientX);
        ch.setAttribute("x1", X(idx)); ch.setAttribute("x2", X(idx));
        ch.setAttribute("visibility", "visible");
        tip.style.display = "block";
        tip.innerHTML = '<b>' + fmtX(vx[idx]) + '</b>' + vs.map(function (s) { return '<br>' + s.name + ' : ' + fmtN(s.values[idx]); }).join("");
        var bx = box.getBoundingClientRect();
        tip.style.left = Math.min(e.clientX - bx.left + 14, bx.width - tip.offsetWidth - 4) + "px";
        tip.style.top = "6px";
      });
      svg.addEventListener("mouseleave", function () {
        tip.style.display = "none"; ch.setAttribute("visibility", "hidden");
      });

      // Brush de zoom
      var startX = null, startIdx = 0;
      function onMove(e) {
        if (startX == null) return;
        var bx = box.getBoundingClientRect();
        var a = Math.min(startX, e.clientX) - bx.left;
        var b = Math.max(startX, e.clientX) - bx.left;
        sel.style.display = "block";
        sel.style.left = a + "px";
        sel.style.width = (b - a) + "px";
      }
      function onUp(e) {
        document.removeEventListener("mousemove", onMove);
        document.removeEventListener("mouseup", onUp);
        chartDragging = false;
        if (startX == null) return;
        var lo = Math.min(startIdx, idxAt(e.clientX)), hi = Math.max(startIdx, idxAt(e.clientX));
        startX = null;
        sel.style.display = "none";
        if (hi - lo >= 1) { box._zx = { min: vx[lo], max: vx[hi] }; paintLine(box); }
      }
      plot.addEventListener("mousedown", function (e) {
        startX = e.clientX; startIdx = idxAt(e.clientX);
        chartDragging = true;
        tip.style.display = "none"; ch.setAttribute("visibility", "hidden");
        document.addEventListener("mousemove", onMove);
        document.addEventListener("mouseup", onUp);
        e.preventDefault();
      });
      svg.addEventListener("dblclick", function () {
        if (box._zx) { box._zx = null; paintLine(box); }
      });
      var rb = box.querySelector(".chart-reset");
      if (rb) rb.addEventListener("click", function () { box._zx = null; paintLine(box); });
    }

    function drawBarChart(box, items) {
      var n = items.length;
      if (!n) { box.innerHTML = ""; return; }
      var W = Math.max(box.clientWidth || 0, 320), H = 170;
      var P = { l: 42, r: 14, t: 10, b: 22 };
      var iw = W - P.l - P.r, ih = H - P.t - P.b;
      var ymax = niceCeil(Math.max(5, ...items.map(function (d) { return d.value; })));
      var bw = Math.min(40, (iw / n) * 0.72);
      var Xc = function (i) { return P.l + ((i + 0.5) / n) * iw; };
      var Y = function (v) { return P.t + ih - (v / ymax) * ih; };
      var muted = cssVar("--muted"), gridc = cssVar("--border"), blue = cssVar("--series-blue");

      var g = "";
      [0, 0.5, 1].forEach(function (f) {
        var yy = Y(ymax * f);
        g += '<line x1="' + P.l + '" y1="' + yy + '" x2="' + (W - P.r) + '" y2="' + yy + '" stroke="' + gridc + '"/>';
        g += '<text x="' + (P.l - 6) + '" y="' + (yy + 3) + '" text-anchor="end" font-size="10" fill="' + muted + '">' + fmtN(Math.round(ymax * f)) + '</text>';
      });
      var bars = items.map(function (d, i) {
        return '<rect x="' + (Xc(i) - bw / 2).toFixed(1) + '" y="' + Y(d.value).toFixed(1) + '" width="' + bw.toFixed(1) + '" height="' + (ih - (Y(d.value) - P.t)).toFixed(1) + '" rx="3" fill="' + blue + '"/>';
      }).join("");
      var labels = items.map(function (d, i) {
        if (n <= 12 || i % Math.ceil(n / 12) === 0) {
          return '<text x="' + Xc(i) + '" y="' + (H - 6) + '" text-anchor="middle" font-size="10" fill="' + muted + '">' + d.label + '</text>';
        }
        return "";
      }).join("");

      box.innerHTML = '<svg viewBox="0 0 ' + W + ' ' + H + '" height="' + H + '">' + g + bars + labels +
        '<rect x="' + P.l + '" y="' + P.t + '" width="' + iw + '" height="' + ih + '" fill="transparent"/>' +
        '</svg><div class="chart-tip"></div>';

      var svg = box.querySelector("svg");
      var tip = box.querySelector(".chart-tip");
      svg.addEventListener("mousemove", function (e) {
        var r = svg.getBoundingClientRect();
        var mx = ((e.clientX - r.left) / r.width) * W;
        var idx = Math.max(0, Math.min(n - 1, Math.floor(((mx - P.l) / iw) * n)));
        tip.style.display = "block";
        tip.innerHTML = items[idx].tip;
        var bx = box.getBoundingClientRect();
        tip.style.left = Math.min(e.clientX - bx.left + 14, bx.width - tip.offsetWidth - 4) + "px";
        tip.style.top = "6px";
      });
      svg.addEventListener("mouseleave", function () {
        tip.style.display = "none";
      });
    }

    function renderLiveChart() {
      if (biTab !== "live" || dayStart == null || chartDragging) return;
      var now = Math.floor(Date.now() / 1000 + clockOffset);
      var xs = [], taken = [], returned = [];
      for (var t = dayStart; t <= now; t += step) {
        xs.push(t);
        var p = actPoints.get(t);
        taken.push(p ? p.taken : 0);
        returned.push(p ? p.returned : 0);
      }
      // On ne trace que la plage oÃ¹ il y a effectivement de la donnÃ©e : on
      // rogne les tranches vides en TÃŠTE (avant les premiÃ¨res donnÃ©es du jour â€”
      // cas du cloud dÃ©marrÃ© en cours de journÃ©e : pas de longue ligne Ã  0
      // depuis 00h00) et en QUEUE (le bucket courant Ã  peine commencÃ©, encore
      // vide â€” Ã©vite la chute verticale Ã  0 au bord droit). Les creux INTERNES
      // (la nuit, une vraie activitÃ© basse mais non nulle) sont conservÃ©s.
      var first = 0, last = xs.length - 1;
      while (first <= last && taken[first] === 0 && returned[first] === 0) first++;
      while (last >= first && taken[last] === 0 && returned[last] === 0) last--;
      if (first > last) { document.getElementById("liveChart").innerHTML = ""; return; }
      drawLineChart(document.getElementById("liveChart"), xs.slice(first, last + 1), [
        { name: "VÃ©los pris", color: cssVar("--series-blue"), values: taken.slice(first, last + 1) },
        { name: "VÃ©los rendus", color: cssVar("--series-green"), values: returned.slice(first, last + 1) },
      ], hhmm);
    }

    async function loadHistory() {
      try {
        var r1 = await fetch("/api/history/daily");
        var r2 = await fetch("/api/history/profile");
        var daily = await r1.json();
        var prof = await r2.json();
      } catch (err) {
        document.getElementById("histEmpty").style.display = "block";
        return;
      }

      if (!daily.days || daily.days.length === 0) {
        document.getElementById("histEmpty").style.display = "block";
        var pb = document.querySelector("#viewHist .panel-body");
        if (pb) pb.style.display = "none";
        return;
      }
      document.getElementById("histEmpty").style.display = "none";
      var pb2 = document.querySelector("#viewHist .panel-body");
      if (pb2) pb2.style.display = "flex";

      drawBarChart(document.getElementById("dailyChart"), daily.days.map(function (d) {
        return {
          label: d.date.slice(8, 10) + "/" + d.date.slice(5, 7),
          value: d.events,
          tip: '<b>' + d.date + '</b><br>' + fmtN(d.events) + ' mvts<br>' + fmtN(d.taken) + ' pris Â· ' + fmtN(d.returned) + ' rendus',
        };
      }));

      drawLineChart(document.getElementById("profChart"),
        prof.hours.map(function (h) { return h.hour; }), [
          { name: "VÃ©los pris", color: cssVar("--series-blue"), values: prof.hours.map(function (h) { return h.taken; }) },
          { name: "VÃ©los rendus", color: cssVar("--series-green"), values: prof.hours.map(function (h) { return h.returned; }) },
        ], function (h) { return h + " h"; });
    }

    /* ========== Interactions DOM ========== */
    var panelEl = document.getElementById("panel");

    document.getElementById("panelBtn").addEventListener("click", function () {
      panelOpen = !panelOpen;
      panelEl.classList.toggle("open", panelOpen);
      document.getElementById("panelBtn").classList.toggle("active", panelOpen);
      // Analyse = plein Ã©cran : on masque la carte (elle n'est plus rendue du
      // tout â†’ fluiditÃ©) ; Ã  la fermeture on la rÃ©affiche et on recalcule sa
      // taille (elle Ã©tait en display:none).
      document.getElementById("map").style.display = panelOpen ? "none" : "";
      if (panelOpen) {
        loadBiTab();
      } else {
        setTimeout(function () { map.invalidateSize(); }, 50);
      }
    });

    function loadBiTab() {
      if (biTab === "kpi") loadKpiTab();
      else if (biTab === "arr") loadArrTab();
      else if (biTab === "biz") loadBizTab();
      else if (biTab === "live") loadActivity();
      else loadHistory();
    }

    function setBiTab(next) {
      biTab = next;
      document.getElementById("tabKpi").classList.toggle("active", biTab === "kpi");
      document.getElementById("tabArr").classList.toggle("active", biTab === "arr");
      document.getElementById("tabBiz").classList.toggle("active", biTab === "biz");
      document.getElementById("tabLive").classList.toggle("active", biTab === "live");
      document.getElementById("tabHist").classList.toggle("active", biTab === "hist");
      document.getElementById("viewKpi").style.display = biTab === "kpi" ? "block" : "none";
      document.getElementById("viewArr").style.display = biTab === "arr" ? "block" : "none";
      document.getElementById("viewBiz").style.display = biTab === "biz" ? "block" : "none";
      document.getElementById("viewLive").style.display = biTab === "live" ? "block" : "none";
      document.getElementById("viewHist").style.display = biTab === "hist" ? "block" : "none";
      document.getElementById("granBtns").style.display = biTab === "live" ? "flex" : "none";
      loadBiTab();
    }

    document.getElementById("tabKpi").addEventListener("click", function () { setBiTab("kpi"); });
    document.getElementById("tabArr").addEventListener("click", function () { setBiTab("arr"); });
    document.getElementById("tabBiz").addEventListener("click", function () { setBiTab("biz"); });
    document.getElementById("tabLive").addEventListener("click", function () { setBiTab("live"); });
    document.getElementById("tabHist").addEventListener("click", function () { setBiTab("hist"); });

    var granBtns = document.querySelectorAll("#granBtns button");
    for (var gi = 0; gi < granBtns.length; gi++) {
      (function (btn) {
        btn.addEventListener("click", function () {
          step = Number(btn.dataset.step);
          for (var bi = 0; bi < granBtns.length; bi++) {
            granBtns[bi].classList.toggle("active", granBtns[bi] === btn);
          }
          loadActivity();
        });
      })(granBtns[gi]);
    }

    var rsTimer = null;
    window.addEventListener("resize", function () {
      clearTimeout(rsTimer);
      rsTimer = setTimeout(function () {
        if (!panelOpen) return;
        if (biTab === "kpi") renderKpiTab();
        else if (biTab === "arr") { arrSvgBuilt = false; loadArrTab(); }
        else if (biTab === "biz") renderBizTab();
        else if (biTab === "live") renderLiveChart();
        else loadHistory();
      }, 200);
    });

    // L'activitÃ© par arrondissement Ã©volue : rafraÃ®chissement doux quand l'onglet est ouvert.
    setInterval(function () {
      if (panelOpen && biTab === "arr") loadArrTab();
    }, 15000);

    function setMode(next) {
      mode = next;
      document.getElementById("modeBikes").classList.toggle("active", mode === "bikes");
      document.getElementById("modeDocks").classList.toggle("active", mode === "docks");
      restyleAll();
      updateMapMode();     // recolore les clusters selon le mode
      renderLegend();
    }

    document.getElementById("modeBikes").addEventListener("click", function () { setMode("bikes"); });
    document.getElementById("modeDocks").addEventListener("click", function () { setMode("docks"); });

    /* ========== Recherche (autocomplÃ©tion live) ========== */
    const searchInput = document.getElementById("search");
    const searchBox = document.getElementById("searchResults");
    let searchMatches = [], searchActive = -1;
    // insensible aux accents ET Ã  la casse : "republique" trouve "RÃ©publique"
    const norm = (s) => s.normalize("NFD").replace(/\p{Diacritic}/gu, "").toLowerCase();

    function closeSearch() {
      searchBox.classList.remove("open");
      searchBox.innerHTML = "";
      searchMatches = []; searchActive = -1;
    }

    function goToStation(st) {
      map.flyTo([st.lat, st.lon], 16, { duration: 0.8 });
      const mk = markers.get(st.station_id);
      if (mk) mk.openPopup();
      searchInput.value = st.name;
      closeSearch();
    }

    function renderSearch() {
      const q = norm(searchInput.value.trim());
      if (q.length < 2) { closeSearch(); return; }
      // prioritÃ© aux noms qui COMMENCENT par la requÃªte, puis ceux qui la contiennent
      const starts = [], has = [];
      for (const st of stations.values()) {
        const n = norm(st.name);
        if (n.startsWith(q)) starts.push(st);
        else if (n.includes(q)) has.push(st);
      }
      searchMatches = starts.concat(has).slice(0, 8);
      searchActive = searchMatches.length ? 0 : -1;
      if (!searchMatches.length) {
        searchBox.innerHTML = '<div class="search-empty">Aucune station trouvÃ©e</div>';
        searchBox.classList.add("open");
        return;
      }
      const noun = mode === "bikes" ? "vÃ©los" : "places";
      searchBox.innerHTML = searchMatches.map(function (st, i) {
        const a = arrOf(st);
        const loc = a ? (a + "áµ‰ arr.") : "banlieue";
        const n = mode === "bikes" ? st.bikes_available : st.docks_available;
        return '<div class="search-item' + (i === 0 ? " active" : "") + '" data-i="' + i + '">' +
          '<span><span class="si-name">' + st.name + '</span><br><span class="si-arr">' + loc + '</span></span>' +
          '<span class="si-count">' + n + " " + noun + '</span></div>';
      }).join("");
      searchBox.classList.add("open");
    }

    function moveSearch(d) {
      if (!searchMatches.length) return;
      searchActive = (searchActive + d + searchMatches.length) % searchMatches.length;
      const items = searchBox.querySelectorAll(".search-item");
      items.forEach(function (el, i) { el.classList.toggle("active", i === searchActive); });
      if (items[searchActive]) items[searchActive].scrollIntoView({ block: "nearest" });
    }

    searchInput.addEventListener("input", renderSearch);
    searchInput.addEventListener("focus", renderSearch);
    searchInput.addEventListener("keydown", function (e) {
      if (e.key === "ArrowDown") { e.preventDefault(); moveSearch(1); }
      else if (e.key === "ArrowUp") { e.preventDefault(); moveSearch(-1); }
      else if (e.key === "Enter") { if (searchMatches[searchActive]) goToStation(searchMatches[searchActive]); }
      else if (e.key === "Escape") { closeSearch(); searchInput.blur(); }
    });
    // mousedown (pas click) + preventDefault : sÃ©lectionne avant que le blur ferme la liste
    searchBox.addEventListener("mousedown", function (e) {
      const item = e.target.closest(".search-item");
      if (item) { e.preventDefault(); goToStation(searchMatches[Number(item.dataset.i)]); }
    });
    document.addEventListener("click", function (e) {
      if (!e.target.closest(".search-wrap")) closeSearch();
    });
  
