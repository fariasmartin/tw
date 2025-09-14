const yogaCenters = [
  { name: "Peace Yoga", city: "Madrid", lat: 40.4168, lng: -3.7038, url: "centers/peace-yoga.html" },
  { name: "Urban Flow", city: "Barcelona", lat: 41.3851, lng: 2.1734, url: "centers/urban-flow.html" },
  { name: "Ocean Zen", city: "Valencia", lat: 39.4699, lng: -0.3763, url: "centers/ocean-zen.html" },
  { name: "Sevilla Breath", city: "Seville", lat: 37.3886, lng: -5.9822, url: "centers/sevilla-breath.html" },
  { name: "Balance Yoga", city: "Bilbao", lat: 43.263, lng: -2.935, url: "centers/balance-yoga.html" },
  { name: "Alma Yoga", city: "Zaragoza", lat: 41.6488, lng: -0.8891, url: "centers/alma-yoga.html" },
  { name: "Costa Zen", city: "Alicante", lat: 38.3452, lng: -0.481, url: "centers/costa-zen.html" },
  { name: "Yoga Norte", city: "Santander", lat: 43.4623, lng: -3.8099, url: "centers/yoga-norte.html" },
  { name: "Soul Stretch", city: "Granada", lat: 37.1773, lng: -3.5986, url: "centers/soul-stretch.html" },
  { name: "Namaste Center", city: "Malaga", lat: 36.7213, lng: -4.4214, url: "centers/namaste-center.html" }
];

const map = L.map('map').setView([40.4168, -3.7038], 6);

L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
  attribution: '&copy; OpenStreetMap contributors'
}).addTo(map);

yogaCenters.forEach(center => {
  const marker = L.marker([center.lat, center.lng]).addTo(map);
  marker.bindPopup(`<a href="${center.url}">${center.name}</a>`);
});

// Optional: Look for a city input field to hook up
document.addEventListener("DOMContentLoaded", () => {
  const input = document.getElementById("citySearch");
  if (!input) return;

  input.addEventListener("change", () => {
    const val = input.value.toLowerCase().trim();
    const matches = yogaCenters.filter(c => c.city.toLowerCase().includes(val));

    if (matches.length > 0) {
      const avgLat = matches.reduce((sum, c) => sum + c.lat, 0) / matches.length;
      const avgLng = matches.reduce((sum, c) => sum + c.lng, 0) / matches.length;
      map.setView([avgLat, avgLng], 12);
    }
  });
});
