/*
 * Shared "OpenAI key has no credits" banner.
 *
 * Drop <script src="js/openai-banner.js"></script> on any page that generates content.
 * It polls /api/system/openai-status and, while blocked, pins a red bar to the top of the
 * page saying so, with the API's own message and an "I topped up" button that clears the
 * flag. Backend-side the generation endpoints answer 409 in the same state, so the banner
 * explains a refusal the user would otherwise have to guess at.
 *
 * Why a banner and not an alert per tool: the key is global. On 2026-07-31 it ran dry and
 * every AI tool silently degraded (v3 falls back to its unpolished deterministic H1), so
 * the signal has to be visible without opening a specific tool.
 */
(function () {
    const ENDPOINT = '/api/system/openai-status';
    const POLL_MS = 60000;
    const ID = 'openaiCreditBanner';
    let clearing = false;

    function fmt(iso) {
        if (!iso) return '';
        // Backend sends an offset-aware ISO string; show it in local time, short form.
        const d = new Date(iso);
        return isNaN(d) ? iso : d.toLocaleString('nl-NL', {
            day: '2-digit', month: '2-digit', hour: '2-digit', minute: '2-digit'
        });
    }

    function remove() {
        const el = document.getElementById(ID);
        if (el) el.remove();
    }

    function render(st) {
        let el = document.getElementById(ID);
        if (!el) {
            el = document.createElement('div');
            el.id = ID;
            // Sticky rather than fixed: no page has to give up padding for it, and it
            // still follows you down a long table.
            el.style.cssText = 'position:sticky; top:0; z-index:1080; background:#fdecea;' +
                'border-bottom:2px solid #d63031; color:#8b1a1a; padding:0.55rem 1rem;' +
                'font-size:0.9rem; display:flex; align-items:center; gap:0.75rem; flex-wrap:wrap;';
            document.body.insertBefore(el, document.body.firstChild);
        }
        el.innerHTML =
            '<strong>OpenAI: geen credits</strong>' +
            '<span>AI-generatie is gestopt' + (st.since ? ' sinds ' + fmt(st.since) : '') +
            '. Kopteksten, FAQ en Unique titles starten niet tot de balance is bijgevuld.</span>' +
            '<button type="button" id="' + ID + 'Btn" class="btn btn-sm" ' +
            'style="border:1px solid #d63031; color:#d63031; background:#fff;">Ik heb bijgevuld</button>' +
            (st.message ? '<span style="opacity:.75; font-size:0.8rem; flex:1 1 100%;">' +
                st.message.replace(/[<>]/g, '') + '</span>' : '');
        document.getElementById(ID + 'Btn').onclick = async function () {
            if (clearing) return;
            clearing = true;
            this.disabled = true;
            this.textContent = 'Controleren…';
            try {
                await fetch(ENDPOINT + '/clear', { method: 'POST' });
                await check();              // re-read; a still-dead key re-blocks on the next call
            } catch (e) {
                this.disabled = false;
                this.textContent = 'Ik heb bijgevuld';
            } finally {
                clearing = false;
            }
        };
    }

    async function check() {
        try {
            const r = await fetch(ENDPOINT);
            if (!r.ok) return;             // never let a status hiccup disturb the page
            const st = await r.json();
            if (st && st.blocked) render(st); else remove();
        } catch (e) { /* offline / backend restarting: stay quiet */ }
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', check);
    } else {
        check();
    }
    setInterval(check, POLL_MS);
})();
