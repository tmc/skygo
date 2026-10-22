// The dashboard's only script. Pages reached by navigation are a static
// shell; this script fetches the view with the session proof header, which
// the browser never sends on its own. The proof lives in this tab's
// sessionStorage, which, unlike the cookie, is private to this origin.
"use strict";
(function () {
  var key = "skygo-session";
  var header = "X-Skygo-Session";

  function proof() {
    try {
      return sessionStorage.getItem(key) || "";
    } catch (e) {
      return "";
    }
  }

  function forget() {
    try {
      sessionStorage.removeItem(key);
    } catch (e) {}
  }

  function toLogin() {
    forget();
    location.replace("/login");
  }

  var form = document.getElementById("login");
  if (form) {
    if (proof()) {
      location.replace("/jobs");
      return;
    }
    form.addEventListener("submit", function (ev) {
      ev.preventDefault();
      var problem = document.getElementById("problem");
      fetch("/login", {
        method: "POST",
        headers: { "X-Skygo-Login": "1" },
        body: new URLSearchParams(new FormData(form)),
        credentials: "same-origin",
        cache: "no-store",
        redirect: "error",
      }).then(function (resp) {
        return resp.text().then(function (text) {
          if (!resp.ok) {
            problem.textContent = text.trim();
            return;
          }
          try {
            sessionStorage.setItem(key, text.trim());
          } catch (e) {
            problem.textContent = "session storage unavailable";
            return;
          }
          location.replace("/jobs");
        });
      }, function () {
        problem.textContent = "dashboard unavailable";
      });
    });
    return;
  }

  var logout = document.getElementById("logout");
  if (logout) {
    logout.addEventListener("click", function () {
      var h = {};
      h[header] = proof();
      fetch("/logout", { method: "POST", headers: h, credentials: "same-origin" }).then(toLogin, toLogin);
    });
  }

  var view = document.getElementById("view");
  if (!view) {
    return;
  }
  var p = proof();
  if (!p) {
    toLogin();
    return;
  }
  var h = {};
  h[header] = p;
  fetch(location.pathname + location.search, {
    headers: h,
    credentials: "same-origin",
    cache: "no-store",
    redirect: "error",
  }).then(function (resp) {
    // Only the plain-text 401 from the session check says the proof itself
    // is no longer valid. Any other refusal, such as 403 when another local
    // page has disturbed the cookie, or a rendered 401 for a rejected
    // endpoint credential, keeps the proof.
    var type = resp.headers.get("Content-Type") || "";
    if (resp.status === 401 && type.indexOf("text/plain") === 0) {
      toLogin();
      return;
    }
    if (!resp.ok && type.indexOf("text/plain") === 0) {
      return resp.text().then(function (text) {
        view.textContent = text.trim();
      });
    }
    if (resp.ok && type.indexOf("text/plain") === 0) {
      // Raw log text, possibly followed: append as inert text.
      var pre = document.createElement("pre");
      view.replaceChildren(pre);
      var reader = resp.body.getReader();
      var dec = new TextDecoder();
      var pump = function () {
        return reader.read().then(function (r) {
          if (r.done) {
            pre.appendChild(document.createTextNode(dec.decode()));
            return;
          }
          pre.appendChild(document.createTextNode(dec.decode(r.value, { stream: true })));
          return pump();
        });
      };
      return pump();
    }
    return resp.text().then(function (html) {
      // Server-rendered by html/template, which escaped every value.
      view.innerHTML = html;
      var h1 = view.querySelector("h1");
      if (h1) {
        document.title = h1.textContent + " · skygo";
      }
    });
  }).catch(function () {
    view.textContent = "dashboard unavailable";
  });
})();
