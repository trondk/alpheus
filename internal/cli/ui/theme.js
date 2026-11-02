// The page's look, set before it is drawn so a dark page does not flash
// light first. Light unless the viewer chose dark (GO-142).
"use strict";
try {
  // ?theme=dark in the address chooses it too, such as in a shared link.
  const asked = new URLSearchParams(location.search).get("theme");
  if (asked === "dark" || asked === "light") localStorage.setItem("alpheus-theme", asked);
  if (localStorage.getItem("alpheus-theme") === "dark") document.documentElement.dataset.theme = "dark";
} catch (e) {
  // No storage, as in a private window: the light look.
}
