(function ($) {
  const COMMONS_LOGO = 'https://upload.wikimedia.org/wikipedia/commons/4/4a/Commons-logo.svg';
  const API = 'https://commons.wikimedia.org/w/api.php';
  const CSS = `
.pb-host { position: relative; overflow: hidden; isolation: isolate; }
.pb-stage { position: absolute; inset: 0; z-index: -1; overflow: hidden; background: #111; }
.pb-layer { position: absolute; inset: 0; background-size: cover; background-position: center; will-change: transform, opacity; }
.pb-shade { position: absolute; inset: 0; }
.pb-credit { position: absolute; right: 8px; bottom: 8px; z-index: 2; font-size: 12px; line-height: 1.4; text-align: right; color: #fff; }
.pb-credit > summary { display: inline-block; list-style: none; cursor: pointer; width: 24px; height: 24px; line-height: 22px; padding: 0; border: 0; border-radius: 50%; background: rgba(0,0,0,.45); color: #fff; text-align: center; font: italic 600 14px/24px Georgia, serif; }
.pb-credit > summary::-webkit-details-marker { display: none; }
.pb-credit > div { margin-top: 6px; padding: 8px 10px; width: max-content; max-width: min(640px, calc(100vw - 32px)); background: rgba(0,0,0,.75); border-radius: 6px; text-align: left; }
.pb-credit div div { white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.pb-credit a, .pb-fcap a { color: #9fe3cf; }
.pb-credit a.pb-link, .pb-fcap a.pb-link { display: block; color: inherit; text-decoration: none; }
.pb-credit a.pb-link:hover, .pb-fcap a.pb-link:hover { color: #9fe3cf; }
.pb-credit button { margin-top: 6px; padding: 4px 10px; font: inherit; color: #fff; background: rgba(255,255,255,.15); border: 1px solid rgba(255,255,255,.4); border-radius: 6px; cursor: pointer; }
.pb-credit button:hover { background: rgba(255,255,255,.3); }
.pb-full { position: fixed; inset: 0; z-index: 99999; background: #000; color: #fff; font: 14px/1.4 system-ui, sans-serif; outline: 0; }
.pb-fcap { position: absolute; left: 16px; bottom: 16px; max-width: min(520px, calc(100vw - 32px)); padding: 8px 12px; background: rgba(0,0,0,.55); border-radius: 6px; opacity: .85; }
.pb-close { position: absolute; top: 12px; right: 12px; width: 36px; height: 36px; font-size: 22px; line-height: 1; color: #fff; background: rgba(0,0,0,.45); border: 0; border-radius: 50%; cursor: pointer; opacity: .6; }
.pb-close:hover { opacity: 1; }`;

  const DEFAULTS = {
    photos: [],
    galleries: [],
    interval: 30000,
    slideshowInterval: 8000,
    zoom: 1.4,
    fade: 1500,
    overlay: 'linear-gradient(rgba(0,0,0,.4), rgba(0,0,0,.15) 55%, rgba(0,0,0,.5))',
    maxPerGallery: 150,
    fullscreen: true,
  };

  const reduced = () => window.matchMedia && matchMedia('(prefers-reduced-motion: reduce)').matches;
  const rand = (a, b) => a + Math.random() * (b - a);
  const plain = html => $('<div>').html(html || '').text().trim();
  const preload = src => new Promise((ok, fail) => { const i = new Image(); i.onload = ok; i.onerror = fail; i.src = src; });

  const categoryOf = link => {
    let s = String(link);
    try { s = decodeURIComponent(s); } catch (e) {}
    const m = s.match(/(Category:[^#?]+)/);
    return m ? m[1].replace(/_/g, ' ') : null;
  };

  async function fetchGallery(link, max) {
    const title = categoryOf(link);
    const out = [];
    let cont = {};
    if (!title) return out;
    while (out.length < max) {
      const data = await $.getJSON(API, Object.assign({
        action: 'query', generator: 'categorymembers', gcmtitle: title, gcmtype: 'file', gcmlimit: 50,
        prop: 'imageinfo', iiprop: 'url|mime|extmetadata', iiurlwidth: 1920,
        iiextmetadatafilter: 'Artist|LicenseShortName|LicenseUrl|ImageDescription|ObjectName',
        format: 'json', origin: '*',
      }, cont));
      Object.values((data.query || {}).pages || {}).forEach(p => {
        const i = (p.imageinfo || [])[0];
        if (!i || !/^image\/(jpeg|png|webp)/.test(i.mime)) return;
        const m = i.extmetadata || {};
        const val = k => (m[k] || {}).value;
        out.push({
          src: i.thumburl || i.url,
          title: plain(val('ObjectName') || val('ImageDescription')) || p.title.replace(/^File:|\.\w+$/g, ''),
          author: plain(val('Artist')),
          license: val('LicenseShortName'),
          licenseUrl: val('LicenseUrl'),
          url: i.descriptionurl,
        });
      });
      if (!data.continue) break;
      cont = data.continue;
    }
    return out;
  }

  function Stage($parent, opts, shade) {
    this.opts = opts;
    this.$el = $('<div class="pb-stage">').appendTo($parent);
    this.$layers = $('<div>').css({ position: 'absolute', inset: 0 }).appendTo(this.$el);
    if (shade) $('<div class="pb-shade">').css('background', shade).appendTo(this.$el);
    this.$cur = null;
    this.onShow = $.noop;
  }

  Stage.prototype.show = function (photo, duration) {
    const fade = this.opts.fade;
    const $layer = $('<div class="pb-layer">').css({ backgroundImage: `url("${photo.src}")`, opacity: 0 }).appendTo(this.$layers);
    const el = $layer[0];
    if (el.animate) {
      if (!reduced()) {
        el.animate([
          { transform: `scale(${this.opts.zoom})`, transformOrigin: '0% 100%', backgroundPosition: '0% 100%' },
          { transform: 'scale(1)', transformOrigin: '100% 0%', backgroundPosition: '100% 0%' },
        ], { duration: duration + fade * 2, easing: 'ease-in-out', fill: 'forwards' });
      }
      el.animate([{ opacity: 0 }, { opacity: 1 }], { duration: fade, fill: 'forwards' });
    } else {
      $layer.css('opacity', 1);
    }
    const prev = this.$cur;
    this.$cur = $layer;
    if (prev) setTimeout(() => prev.remove(), fade + 100);
    this.onShow(photo);
  };

  function fillCaption($box, p) {
    const $wrap = p.url
      ? $('<a class="pb-link" target="_blank" rel="noopener" title="View on Wikimedia Commons">').attr('href', p.url)
      : $('<div>');
    $wrap.append($('<div>').text(p.title || ''));
    const $by = $('<div>').appendTo($wrap);
    const parts = [p.author && '© ' + p.author, p.license].filter(Boolean);
    $by.text(parts.join(' · '));
    if (p.url) {
      if (parts.length) $by.append(' · ');
      $by.append($('<img alt="Wikimedia Commons">').attr('src', COMMONS_LOGO).css({ height: 16, verticalAlign: 'text-bottom' }));
    }
    $box.empty().append($wrap);
  }

  function Backdrop($host, opts) {
    this.$host = $host;
    this.opts = opts;
    this.pool = opts.photos.slice();
    this.last = null;
    this.timer = null;
    if (!$('#pb-css').length) $('<style id="pb-css">').text(CSS).appendTo('head');
    $host.addClass('pb-host');
    this.stage = new Stage($host, opts, opts.overlay);
    this.buildCredit();
    this.start();
  }

  Backdrop.prototype.buildCredit = function () {
    const self = this;
    const $d = $('<details class="pb-credit">').appendTo(this.$host);
    $('<summary aria-label="Image credit" title="Image credit">').text('i').appendTo($d);
    const $box = $('<div>').appendTo($d);
    this.$caption = $('<div>').appendTo($box);
    if (this.opts.fullscreen) $('<button type="button">').text('⛶ View Full Screen').on('click', () => self.openSlideshow()).appendTo($box);
    this.stage.onShow = p => fillCaption(self.$caption, p);
    const collapse = () => $d.prop('open', false);
    $d.on('focusout', e => { if (e.relatedTarget && !$d[0].contains(e.relatedTarget)) collapse(); });
    $(document).on('pointerdown', e => { if (!$d[0].contains(e.target)) collapse(); });
    $d.on('keydown', e => { if (e.key === 'Escape') collapse(); });
  };

  Backdrop.prototype.pick = function () {
    const choices = this.pool.length > 1 ? this.pool.filter(p => p !== this.last) : this.pool;
    return choices[Math.floor(Math.random() * choices.length)];
  };

  Backdrop.prototype.advance = async function (stage, interval, first) {
    for (let tries = 0; tries < 5; tries++) {
      const p = first && tries === 0 ? first : this.pick();
      if (!p) return;
      try { await preload(p.src); } catch (e) { this.pool = this.pool.filter(x => x !== p); continue; }
      this.last = p;
      stage.show(p, interval);
      return;
    }
  };

  Backdrop.prototype.loop = function (stage, interval, first) {
    const self = this;
    const tick = async f => {
      await self.advance(stage, interval, f);
      self.timer = setTimeout(tick, interval);
    };
    clearTimeout(this.timer);
    tick(first);
  };

  Backdrop.prototype.start = function () {
    const self = this;
    const galleries = Promise.all(this.opts.galleries.map(g => fetchGallery(g, self.opts.maxPerGallery).catch(() => [])))
      .then(lists => {
        const seen = new Set(self.pool.map(p => p.url || p.src));
        lists.forEach(list => list.forEach(p => { if (!seen.has(p.url || p.src)) { seen.add(p.url || p.src); self.pool.push(p); } }));
      });
    const go = () => self.loop(self.stage, self.opts.interval, self.opts.photos[0]);
    if (this.pool.length) go(); else galleries.then(go);
  };

  Backdrop.prototype.openSlideshow = function () {
    const self = this;
    const o = this.opts;
    let timer, closed = false;
    clearTimeout(this.timer);
    const $ov = $('<div class="pb-full" tabindex="-1">').appendTo('body');
    const stage = new Stage($ov, o, null);
    const $cap = $('<div class="pb-fcap">').appendTo($ov);
    const $close = $('<button type="button" class="pb-close" aria-label="Exit slideshow">').text('×').appendTo($ov);
    stage.onShow = p => fillCaption($cap, p);

    const tick = async () => {
      await self.advance(stage, o.slideshowInterval);
      clearTimeout(timer);
      timer = setTimeout(tick, o.slideshowInterval);
    };
    const close = () => {
      if (closed) return;
      closed = true;
      clearTimeout(timer);
      $(document).off('.pbfull');
      if (document.fullscreenElement) document.exitFullscreen().catch($.noop);
      $ov.remove();
      self.loop(self.stage, o.interval);
    };

    $close.on('click', close);
    $(document).on('keydown.pbfull', e => {
      if (e.key === 'Escape') close();
      if (e.key === 'ArrowRight') { clearTimeout(timer); tick(); }
    });
    $(document).on('fullscreenchange.pbfull', () => { if (!document.fullscreenElement) close(); });
    if ($ov[0].requestFullscreen) $ov[0].requestFullscreen().catch($.noop);
    $ov.trigger('focus');
    tick();
  };

  $.fn.photoBackdrop = function (options) {
    return this.each(function () {
      if ($.data(this, 'photoBackdrop')) return;
      $.data(this, 'photoBackdrop', new Backdrop($(this), $.extend({}, DEFAULTS, options)));
    });
  };
})(jQuery);
