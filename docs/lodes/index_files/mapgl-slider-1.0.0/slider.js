/**
 * Time Slider Control for mapgl (Mapbox GL JS and MapLibre GL JS).
 *
 * Implements the IControl interface so it participates in the native
 * control positioning system (stacks beside navigation, scale, etc).
 *
 * Composes with the shared per-layer filter registry installed by
 * mapboxgl.js / maplibregl.js:
 *   - window._mapglEnsureLayerState(map)
 *   - window._mapglComposeFilter(map, layerId)
 * Writes the slider's filter to filterStack[layer].slider; base / user /
 * legend slots from other sources continue to compose on every tick.
 *
 * Public surface:
 *   new SliderControl(options)
 *     .onAdd(map), .onRemove()
 *     .targetsLayer(layerId)   -- bool; for proxy add_layer wiring
 *     .currentFilter()         -- returns the current slider expression
 *     .update({ value, playing, animation_duration })  -- proxy updater
 */
(function () {
    "use strict";

    // Inline SVG icons. currentColor lets CSS theme them.
    var ICON_PLAY =
        '<svg viewBox="0 0 16 16" width="14" height="14" aria-hidden="true">' +
        '<path fill="currentColor" d="M4 2.5v11l10-5.5z"/></svg>';
    var ICON_PAUSE =
        '<svg viewBox="0 0 16 16" width="14" height="14" aria-hidden="true">' +
        '<path fill="currentColor" d="M4 2h3v12H4zM9 2h3v12H9z"/></svg>';

    function buildFilter(mode, property, value) {
        if (mode === "cumulative") {
            return ["<=", ["get", property], value];
        }
        // default: sequential / equality
        return ["==", ["get", property], value];
    }

    function SliderControl(options) {
        this.options = options || {};
        this._layers = (options.layers || []).slice();
        this._property = options.property;
        this._values = (options.values || []).slice();
        this._labels = (options.labels && options.labels.length === this._values.length)
            ? options.labels.slice()
            : this._values.map(function (v) { return String(v); });
        this._mode = options.mode === "cumulative" ? "cumulative" : "sequential";
        this._index = this._clampIndex(options.initial_index || 0);
        this._playButton = !!options.play_button;
        this._animationDuration = Math.max(50, options.animation_duration || 1000);
        this._loop = options.loop !== false;
        this._title = options.title || null;
        this._showValue = options.show_value !== false;
        this._width = options.width || 280;
        this._background = options.background_color || "#ffffffcc";
        this._textColor = options.text_color || "#404040";
        this._accent = options.accent_color || "#4a90e2";
        this._playing = false;
        this._timer = null;
        // Shiny input name: either explicitly provided or derived from the
        // map container id in onAdd as `<id>_slider`.
        this._shinyInputName = options.shiny_input_name || null;
        // Paint-property animation (optional). The R side normalizes both
        // the single-form (paint_property + paint_expressions) and the
        // multi-form (paint_properties) into a single dict shape:
        //   paint_properties: { "fill-color": [expr0, expr1, ...], ... }
        // For backwards compat with any payload still in the single form,
        // we also accept paint_property + paint_expressions here.
        this._paintProperties = {};
        if (options.paint_properties && typeof options.paint_properties === "object") {
            for (var pn in options.paint_properties) {
                if (Object.prototype.hasOwnProperty.call(options.paint_properties, pn)) {
                    this._paintProperties[pn] = (options.paint_properties[pn] || []).slice();
                }
            }
        } else if (options.paint_property && options.paint_expressions) {
            this._paintProperties[options.paint_property] =
                (options.paint_expressions || []).slice();
        }
        // Per-layer, per-property baseline captured on first apply.
        // Shape: { layer_id: { property_name: <original value> } }
        this._paintBaseline = {};
    }

    SliderControl.prototype._clampIndex = function (i) {
        if (!this._values.length) return 0;
        if (i < 0) return 0;
        if (i >= this._values.length) return this._values.length - 1;
        return i;
    };

    SliderControl.prototype.targetsLayer = function (layerId) {
        return this._layers.indexOf(layerId) !== -1;
    };

    SliderControl.prototype.currentFilter = function () {
        if (!this._values.length || !this._property) return null;
        return buildFilter(this._mode, this._property, this._values[this._index]);
    };

    // Return { property: expression } for the current step, or null if
    // paint isn't configured.
    SliderControl.prototype._currentPaintOverrides = function () {
        var props = Object.keys(this._paintProperties);
        if (!props.length) return null;
        var out = {};
        for (var i = 0; i < props.length; i++) {
            var list = this._paintProperties[props[i]];
            if (list && list.length) out[props[i]] = list[this._index];
        }
        return out;
    };

    // Capture baseline(s) and apply current paint expression(s) for a
    // single layer. Safe to call multiple times for the same layer (the
    // per-property baseline is captured only on first sighting). Called
    // from the late-added-layer hook in the binding files and indirectly
    // from the main apply path via the same per-layer branch.
    SliderControl.prototype.captureAndApplyPaintForLayer = function (layerId) {
        if (!this._map) return;
        var props = Object.keys(this._paintProperties);
        if (!props.length) return;
        if (!this._map.getLayer || !this._map.getLayer(layerId)) return;
        if (!this._paintBaseline[layerId]) this._paintBaseline[layerId] = {};
        for (var i = 0; i < props.length; i++) {
            var prop = props[i];
            var list = this._paintProperties[prop];
            if (!list || !list.length) continue;
            if (!Object.prototype.hasOwnProperty.call(this._paintBaseline[layerId], prop)) {
                // undefined = no explicit value; setPaintProperty(..., undefined)
                // restores the style default on removal.
                this._paintBaseline[layerId][prop] = this._map.getPaintProperty(layerId, prop);
            }
            this._map.setPaintProperty(layerId, prop, list[this._index]);
        }
    };

    SliderControl.prototype.onAdd = function (map) {
        this._map = map;
        // Enforce one-slider-per-map: quietly replace any prior instance.
        if (map._mapglSliderControl && map._mapglSliderControl !== this) {
            try { map.removeControl(map._mapglSliderControl); } catch (e) { /* noop */ }
        }
        map._mapglSliderControl = this;
        // Derive the Shiny input name from the map container id if not
        // supplied explicitly.
        if (!this._shinyInputName) {
            var _cid = (map.getContainer && map.getContainer().id) || null;
            if (_cid) this._shinyInputName = _cid + "_slider";
        }

        var self = this;

        // Build DOM.
        // The outer node uses .mapboxgl-ctrl so native positioning applies;
        // the inner wrapper is what we style. Inline width/colors come from
        // options so we don't need a style injection step for each instance.
        var root = document.createElement("div");
        root.className = "mapboxgl-ctrl maplibregl-ctrl mapgl-slider";
        root.style.width = this._width + "px";
        root.style.background = this._background;
        root.style.color = this._textColor;

        // CSS custom property powers the accent color in the range thumb.
        root.style.setProperty("--mapgl-slider-accent", this._accent);

        var body = document.createElement("div");
        body.className = "mapgl-slider-body";

        if (this._title) {
            var titleEl = document.createElement("div");
            titleEl.className = "mapgl-slider-title";
            titleEl.textContent = this._title;
            body.appendChild(titleEl);
        }

        var header = document.createElement("div");
        header.className = "mapgl-slider-header";

        var valueEl = document.createElement("div");
        valueEl.className = "mapgl-slider-value";
        if (!this._showValue) valueEl.style.display = "none";
        valueEl.textContent = this._labels[this._index] || "";
        header.appendChild(valueEl);
        this._valueEl = valueEl;

        body.appendChild(header);

        var row = document.createElement("div");
        row.className = "mapgl-slider-row";

        if (this._playButton) {
            var playBtn = document.createElement("button");
            playBtn.type = "button";
            playBtn.className = "mapgl-slider-play";
            playBtn.setAttribute("aria-label", "Play");
            playBtn.innerHTML = ICON_PLAY;
            playBtn.addEventListener("click", function () {
                self._togglePlay();
            });
            row.appendChild(playBtn);
            this._playBtn = playBtn;
        }

        var input = document.createElement("input");
        input.type = "range";
        input.className = "mapgl-slider-input";
        input.min = "0";
        input.max = String(Math.max(0, this._values.length - 1));
        input.step = "1";
        input.value = String(this._index);
        // Pause when the user scrubs.
        input.addEventListener("input", function (e) {
            self._setIndex(parseInt(e.target.value, 10), { pause: true, fromUser: true });
        });
        row.appendChild(input);
        this._input = input;

        body.appendChild(row);
        root.appendChild(body);
        this._root = root;

        // Seed filter registry slots for each target layer and apply.
        // Initial mount uses the synchronous path so the first paint
        // already has the filter applied (no flash of unfiltered data).
        this._applyFilterNow();

        // Fire initial Shiny input value if applicable.
        this._fireShinyInput();

        // Keep the slider filter applied even if the style reloads.
        this._onStyleData = function () {
            // Style reloads re-run the replay in state.filters, which
            // includes our composed expression. But filterStack persists
            // on window so composeAndApplyFilter remains correct; just
            // re-trigger to be safe in case any layer got re-created with
            // only its `base` filter. Immediate rather than coalesced so
            // we never leave a stale filter visible.
            self._applyFilterNow();
        };
        map.on("styledata", this._onStyleData);

        return root;
    };

    SliderControl.prototype.onRemove = function () {
        this._stopLoop();
        if (this._map) {
            if (this._onStyleData) {
                try { this._map.off("styledata", this._onStyleData); } catch (e) { /* noop */ }
            }
            // Release slider slot for every targeted layer.
            var state = (typeof window._mapglEnsureLayerState === "function")
                ? window._mapglEnsureLayerState(this._map)
                : null;
            if (state) {
                for (var i = 0; i < this._layers.length; i++) {
                    var lid = this._layers[i];
                    if (state.filterStack[lid]) {
                        state.filterStack[lid].slider = null;
                    }
                    if (typeof window._mapglComposeFilter === "function") {
                        window._mapglComposeFilter(this._map, lid);
                    }
                }
            }
            // Restore every captured paint baseline, across all layers
            // and all configured paint properties. Passing undefined to
            // setPaintProperty restores the style default, which matches
            // the original "no explicit value" state.
            for (var baseLid in this._paintBaseline) {
                if (
                    !Object.prototype.hasOwnProperty.call(
                        this._paintBaseline,
                        baseLid
                    )
                ) continue;
                if (!this._map.getLayer || !this._map.getLayer(baseLid)) continue;
                var layerBaselines = this._paintBaseline[baseLid];
                for (var bProp in layerBaselines) {
                    if (
                        Object.prototype.hasOwnProperty.call(layerBaselines, bProp)
                    ) {
                        this._map.setPaintProperty(
                            baseLid,
                            bProp,
                            layerBaselines[bProp]
                        );
                    }
                }
            }
            if (this._map._mapglSliderControl === this) {
                delete this._map._mapglSliderControl;
            }
        }
        if (this._root && this._root.parentNode) {
            this._root.parentNode.removeChild(this._root);
        }
        this._map = null;
    };

    // Coalesce filter applies to one-per-animation-frame. On dense
    // vector tile / PMTiles layers, map.setFilter triggers a repaint
    // that can queue up faster than the renderer paints; without this,
    // fast dragging leaves the map trailing the slider. rAF coalescing
    // collapses multiple drag ticks into a single setFilter per paint
    // tick, reading the most recent index when the frame actually fires.
    SliderControl.prototype._applyFilterToLayers = function () {
        if (!this._map || !this._values.length) return;
        if (this._rafPending) return;
        var self = this;
        this._rafPending = true;
        var raf =
            (typeof window !== "undefined" && window.requestAnimationFrame) ||
            function (fn) { return setTimeout(fn, 16); };
        raf(function () {
            self._rafPending = false;
            self._applyFilterNow();
        });
    };

    // Synchronous application path. Called from the rAF callback and on
    // onAdd / styledata / onRemove where we want immediate effect.
    // Applies filter (if `property` is configured) and/or paint (if
    // paint properties are configured). Either, both, or neither may run
    // per tick depending on how the slider was configured.
    SliderControl.prototype._applyFilterNow = function () {
        if (!this._map || !this._values.length) return;
        var hasFilter = this._property != null;
        var paintProps = Object.keys(this._paintProperties);
        var hasPaint = paintProps.length > 0;
        var filter = this.currentFilter();
        var state = (typeof window._mapglEnsureLayerState === "function")
            ? window._mapglEnsureLayerState(this._map)
            : null;
        for (var i = 0; i < this._layers.length; i++) {
            var lid = this._layers[i];

            // ---- filter branch ----
            if (hasFilter) {
                if (state) {
                    state.filterStack[lid] = state.filterStack[lid] || {};
                    state.filterStack[lid].slider = filter;
                    if (typeof window._mapglComposeFilter === "function") {
                        window._mapglComposeFilter(this._map, lid);
                    } else if (this._map.getLayer && this._map.getLayer(lid)) {
                        // Legacy fallback: no composition with other sources.
                        this._map.setFilter(lid, filter);
                    }
                }
            }

            // ---- paint branch ----
            // Loop every configured paint property; capture baseline per
            // (layer, property) on first sighting, then apply current expr.
            if (hasPaint && this._map.getLayer && this._map.getLayer(lid)) {
                if (!this._paintBaseline[lid]) this._paintBaseline[lid] = {};
                for (var p = 0; p < paintProps.length; p++) {
                    var prop = paintProps[p];
                    var list = this._paintProperties[prop];
                    if (!list || !list.length) continue;
                    if (
                        !Object.prototype.hasOwnProperty.call(
                            this._paintBaseline[lid],
                            prop
                        )
                    ) {
                        this._paintBaseline[lid][prop] =
                            this._map.getPaintProperty(lid, prop);
                    }
                    this._map.setPaintProperty(lid, prop, list[this._index]);
                }
            }
        }
    };

    SliderControl.prototype._setIndex = function (index, opts) {
        opts = opts || {};
        var clamped = this._clampIndex(index);
        var changed = clamped !== this._index;
        this._index = clamped;
        if (this._input) this._input.value = String(clamped);
        if (this._valueEl) this._valueEl.textContent = this._labels[clamped] || "";
        if (opts.pause) this._stopLoop();
        if (changed || opts.force) {
            this._applyFilterToLayers();
            this._fireShinyInput();
        }
    };

    SliderControl.prototype._fireShinyInput = function () {
        if (!this._shinyInputName) return;
        if (typeof Shiny === "undefined" || !Shiny.setInputValue) return;
        Shiny.setInputValue(
            this._shinyInputName,
            {
                value: this._values[this._index],
                index: this._index,
                label: this._labels[this._index],
                playing: this._playing
            },
            { priority: "event" }
        );
    };

    SliderControl.prototype._togglePlay = function () {
        if (this._playing) this._stopLoop(); else this._startLoop();
    };

    SliderControl.prototype._startLoop = function () {
        if (this._playing || this._values.length < 2) return;
        this._playing = true;
        this._setPlayIcon(true);
        var self = this;
        var step = function () {
            if (!self._playing) return;
            var next = self._index + 1;
            if (next >= self._values.length) {
                if (!self._loop) { self._stopLoop(); return; }
                next = 0;
            }
            self._setIndex(next, { force: true });
            self._timer = setTimeout(step, self._animationDuration);
        };
        this._timer = setTimeout(step, this._animationDuration);
        this._fireShinyInput();
    };

    SliderControl.prototype._stopLoop = function () {
        if (this._timer) { clearTimeout(this._timer); this._timer = null; }
        if (!this._playing) return;
        this._playing = false;
        this._setPlayIcon(false);
        this._fireShinyInput();
    };

    SliderControl.prototype._setPlayIcon = function (playing) {
        if (!this._playBtn) return;
        this._playBtn.innerHTML = playing ? ICON_PAUSE : ICON_PLAY;
        this._playBtn.setAttribute("aria-label", playing ? "Pause" : "Play");
    };

    // Proxy-driven updates from R's update_slider_control().
    SliderControl.prototype.update = function (msg) {
        if (msg == null) return;
        if (msg.animation_duration != null) {
            this._animationDuration = Math.max(50, msg.animation_duration);
        }
        if (msg.value != null) {
            // Find the matching index for this value. Use strict equality;
            // values are expected to be numeric.
            var idx = -1;
            for (var i = 0; i < this._values.length; i++) {
                if (this._values[i] === msg.value) { idx = i; break; }
            }
            // Fallback: if exact match fails (e.g., float imprecision), pick
            // the nearest numeric value.
            if (idx === -1) {
                var bestDelta = Infinity;
                for (var j = 0; j < this._values.length; j++) {
                    var d = Math.abs(Number(this._values[j]) - Number(msg.value));
                    if (d < bestDelta) { bestDelta = d; idx = j; }
                }
            }
            if (idx >= 0) this._setIndex(idx, { force: true });
        }
        if (msg.playing === true && !this._playing) this._startLoop();
        if (msg.playing === false && this._playing) this._stopLoop();
    };

    // Expose globally for the binding files to instantiate.
    window.MapglSliderControl = SliderControl;
})();
