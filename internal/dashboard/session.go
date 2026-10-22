package dashboard

import (
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"time"
)

const (
	cookiePrefix    = "skygo_dashboard_" // followed by the bound port
	codeLifetime    = 10 * time.Minute
	codeAttempts    = 5
	idleLifetime    = 30 * time.Minute
	sessionLifetime = 8 * time.Hour
)

var (
	errCodeWrong = errors.New("incorrect login code")
	errCodeSpent = errors.New("login code is no longer valid; restart skygo dashboard for a new code")
)

// loginCode is the process's one-time code. It is spent by one successful
// login, by codeAttempts wrong ones, or by expiry.
type loginCode struct {
	value    string
	expires  time.Time
	attempts int
	used     bool
}

// session is the one session a process can mint. It exists only in memory.
//
// It has two secrets. The token is the HttpOnly cookie, which browsers send
// to every port of the host, so another local server the browser visits can
// receive it. The proof is returned once, to the login script, which keeps it
// in this origin's sessionStorage and sends it as proofHeader; browsers never
// send it on their own. Data needs both. The cookie name includes the bound
// port so that two dashboards do not replace each other's cookie.
type session struct {
	token, proof  string
	created, used time.Time
}

func randomHex(r io.Reader, n int) (string, error) {
	b := make([]byte, n)
	if _, err := io.ReadFull(r, b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// login spends the login code on a correct guess and returns the new
// session. A wrong guess counts toward the attempt limit.
func (s *server) login(guess string) (session, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	c := &s.code
	if c.used || c.attempts >= codeAttempts || !now.Before(c.expires) {
		return session{}, errCodeSpent
	}
	if subtle.ConstantTimeCompare([]byte(guess), []byte(c.value)) != 1 {
		c.attempts++
		return session{}, errCodeWrong
	}
	c.used = true
	token, err := randomHex(s.random, 32)
	if err != nil {
		return session{}, err
	}
	proof, err := randomHex(s.random, 32)
	if err != nil {
		return session{}, err
	}
	s.session = &session{token: token, proof: proof, created: now, used: now}
	return *s.session, nil
}

// codeAvailable reports whether a login can still succeed.
func (s *server) codeAvailable() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := s.code
	return !c.used && c.attempts < codeAttempts && s.now().Before(c.expires)
}

// authenticate reports whether r carries the live session cookie and
// whether it carries the live session proof. Only a request with both
// extends the idle lifetime. An expired session is forgotten.
//
// Every cookie with the session name is checked: a page on another port of
// the host can plant a same-named cookie with a longer path, which browsers
// send first, beside the real one.
func (s *server) authenticate(r *http.Request) (cookie, proof bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur := s.session
	if cur == nil {
		return false, false
	}
	now := s.now()
	if !now.Before(cur.used.Add(idleLifetime)) || !now.Before(cur.created.Add(sessionLifetime)) {
		s.session = nil
		return false, false
	}
	for _, c := range r.CookiesNamed(s.cookie) {
		if subtle.ConstantTimeCompare([]byte(c.Value), []byte(cur.token)) == 1 {
			cookie = true
		}
	}
	proof = subtle.ConstantTimeCompare([]byte(r.Header.Get(proofHeader)), []byte(cur.proof)) == 1
	if cookie && proof {
		cur.used = now
	}
	return cookie, proof
}

type loginView struct {
	Available bool
	Problem   string
}

// loginForm shows the login form while the code can be used. It never
// redirects: a tab without the proof cannot use an existing session.
func (s *server) loginForm(w http.ResponseWriter, r *http.Request) {
	v := loginView{Available: s.codeAvailable()}
	if !v.Available {
		v.Problem = errCodeSpent.Error()
	}
	s.render(w, http.StatusOK, "login", "log in", v)
}

// loginSubmit reads the code only from the POST body, which ServeHTTP has
// bounded, never from the URL. Only the dashboard's script can log in: it
// sets loginHeader, and it receives the session proof as the response body.
func (s *server) loginSubmit(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get(loginHeader) != "1" {
		http.Error(w, "log in with JavaScript enabled", http.StatusBadRequest)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, "invalid login form", http.StatusBadRequest)
		return
	}
	guess := r.PostForm.Get("code")
	if guess == "" {
		http.Error(w, "login code required", http.StatusBadRequest)
		return
	}
	sess, err := s.login(guess)
	if err != nil {
		text := err.Error()
		if !errors.Is(err, errCodeWrong) && !errors.Is(err, errCodeSpent) {
			text = "login failed"
		}
		http.Error(w, text, http.StatusForbidden)
		return
	}
	http.SetCookie(w, &http.Cookie{Name: s.cookie, Value: sess.token, Path: "/", HttpOnly: true, SameSite: http.SameSiteStrictMode})
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	io.WriteString(w, sess.proof)
}

func (s *server) logout(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.session = nil
	s.mu.Unlock()
	http.SetCookie(w, &http.Cookie{Name: s.cookie, Value: "", Path: "/", MaxAge: -1, HttpOnly: true, SameSite: http.SameSiteStrictMode})
	http.Redirect(w, r, "/login", http.StatusSeeOther)
}
