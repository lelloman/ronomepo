//! The plugin stylesheet and the small widget builders every view shares.
//!
//! Styling follows the host's New UI chrome (JetBrains style): it only uses
//! the host's named colors (`@text_0`, `@accent`, ...), so the palette the user
//! picks in Maruzzella keeps applying, and every class is `ronomepo-` prefixed.

use std::cell::Cell;

use gtk::pango::EllipsizeMode;
use gtk::prelude::*;
use gtk::{Align, Box as GtkBox, Button, Grid, Image, Label, Orientation, ToggleButton, Widget};
use maruzzella_sdk::{button_css_class, text_css_class};

use crate::mark_clickable;

const STYLESHEET: &str = r#"
/* Text tones */
.ronomepo-fg-strong { color: @text_0; }
.ronomepo-fg-neutral { color: @text_1; }
.ronomepo-fg-muted { color: @text_2; }
.ronomepo-fg-accent { color: @accent_strong; }
.ronomepo-fg-success { color: @success; }
.ronomepo-fg-warning { color: @warning; }
.ronomepo-fg-danger { color: @danger; }
.ronomepo-meta { font-size: 12px; }
.ronomepo-small { font-size: 11px; }
.ronomepo-page-title { font-size: 16px; font-weight: 600; color: @text_0; }
.ronomepo-section-title { font-weight: 600; color: @text_0; }
.ronomepo-strong { font-weight: 600; }
.ronomepo-italic { font-style: italic; }

/* Badges: small, low-contrast rounded chips */
.ronomepo-badge {
  padding: 0 6px;
  min-height: 18px;
  border-radius: 4px;
  font-size: 11px;
  font-weight: 500;
  background: alpha(@text_0, 0.08);
  color: @text_1;
}
.ronomepo-badge.ronomepo-tone-accent { background: alpha(@accent, 0.22); color: @accent_strong; }
.ronomepo-badge.ronomepo-tone-success { background: alpha(@success, 0.14); color: @success; }
.ronomepo-badge.ronomepo-tone-warning { background: alpha(@warning, 0.14); color: @warning; }
.ronomepo-badge.ronomepo-tone-danger { background: alpha(@danger, 0.16); color: @danger; }
.ronomepo-badge.ronomepo-tone-muted { background: alpha(@text_0, 0.05); color: @text_2; }

/* Underline tabs (tool window and editor-area tab headers) */
.ronomepo-tab-bar { border-bottom: 1px solid @border; padding: 0 8px; }
button.ronomepo-tab {
  min-height: 32px;
  padding: 0 10px;
  margin: 0;
  border: 0;
  border-radius: 0;
  background: transparent;
  box-shadow: inset 0 -2px transparent;
}
button.ronomepo-tab label { color: @text_1; font-weight: 400; }
button.ronomepo-tab:hover { background: alpha(@text_0, 0.05); }
button.ronomepo-tab:hover label,
button.ronomepo-tab:checked label { color: @text_0; }
button.ronomepo-tab:checked { background: transparent; box-shadow: inset 0 -2px @accent; }

/* Filter chips */
button.ronomepo-chip {
  min-height: 22px;
  padding: 0 8px;
  border-radius: 4px;
  border: 1px solid transparent;
  background: transparent;
  box-shadow: none;
}
button.ronomepo-chip label { color: @text_1; font-size: 12px; font-weight: 400; }
button.ronomepo-chip:hover { background: alpha(@text_0, 0.07); }
button.ronomepo-chip:checked { background: alpha(@accent, 0.18); border-color: alpha(@accent, 0.55); }
button.ronomepo-chip:checked label { color: @text_0; }

/* Flat icon buttons, as in tool window toolbars */
button.ronomepo-icon-button {
  min-width: 26px;
  min-height: 26px;
  padding: 0;
  border: 0;
  border-radius: 4px;
  background: transparent;
  box-shadow: none;
}
button.ronomepo-icon-button image { color: @text_1; -gtk-icon-size: 16px; }
button.ronomepo-icon-button:hover { background: alpha(@text_0, 0.08); }
button.ronomepo-icon-button:checked { background: alpha(@text_0, 0.14); }
button.ronomepo-icon-button:disabled image { color: alpha(@text_2, 0.5); }

/* Link-style actions */
button.ronomepo-link {
  min-height: 22px;
  padding: 0 2px;
  border: 0;
  background: transparent;
  box-shadow: none;
}
button.ronomepo-link label { color: @accent_strong; font-weight: 400; }
button.ronomepo-link:hover { background: transparent; }
button.ronomepo-link:hover label { text-decoration-line: underline; }
button.ronomepo-link:disabled label { color: @text_2; }

/* Compact, borderless dropdowns */
dropdown.ronomepo-dropdown > button {
  min-height: 24px;
  padding: 0 6px;
  border: 0;
  border-radius: 4px;
  background: transparent;
  box-shadow: none;
}
dropdown.ronomepo-dropdown > button:hover { background: alpha(@text_0, 0.08); }
dropdown.ronomepo-dropdown > button label { color: @text_1; font-size: 12px; font-weight: 400; }

/* Tool window chrome */
.ronomepo-toolwindow-header { min-height: 34px; padding: 0 6px 0 12px; border-bottom: 1px solid @border; }
.ronomepo-toolwindow-header button.ronomepo-tab { min-height: 34px; }
.ronomepo-toolwindow-title { color: @text_0; font-weight: 600; margin-right: 8px; }
.ronomepo-filter-bar { padding: 6px 8px; }

/* Dense tables and tree-like lists */
.ronomepo-table-header { min-height: 24px; padding: 0 12px; border-bottom: 1px solid @border; }
.ronomepo-table-header label { color: @text_2; font-size: 11px; font-weight: 500; }
list.ronomepo-tree { background: transparent; }
list.ronomepo-tree > row { min-height: 24px; }
.ronomepo-tree-row { min-height: 24px; padding: 0 12px; }
.ronomepo-row-name { color: @text_0; }
.ronomepo-vcs-modified { color: @accent_strong; }
.ronomepo-vcs-unversioned { color: @danger; }
.ronomepo-vcs-missing { color: @text_2; font-style: italic; }
.ronomepo-vcs-unknown { color: @text_1; }

/* Pages (editor area) */
.ronomepo-page-header { padding: 14px 24px 10px; }
.ronomepo-card {
  background: alpha(@text_0, 0.03);
  border: 1px solid @border;
  border-radius: 6px;
  padding: 10px 12px;
}
/* Containers inside cards must not repaint the pane background. */
.ronomepo-card box, .ronomepo-card grid,
.ronomepo-list-card box, .ronomepo-list-card grid,
.ronomepo-stat-tile box, .ronomepo-info-bar box { background: transparent; }
.ronomepo-list-card { background: alpha(@text_0, 0.02); border: 1px solid @border; border-radius: 6px; }
.ronomepo-list-card > .ronomepo-list-item { padding: 8px 12px; border-bottom: 1px solid alpha(@border, 0.7); }
.ronomepo-list-card > .ronomepo-list-item:last-child { border-bottom: 0; }
.ronomepo-list-card > .ronomepo-list-item:hover { background: alpha(@text_0, 0.03); }
.ronomepo-kv-key { color: @text_2; }
.ronomepo-kv-value { color: @text_0; }
.ronomepo-problem { padding: 3px 0; }
.ronomepo-problem image { -gtk-icon-size: 16px; }
image.ronomepo-fg-danger { color: @danger; }
image.ronomepo-fg-warning { color: @warning; }
image.ronomepo-fg-muted { color: @text_2; }
image.ronomepo-fg-accent { color: @accent_strong; }
image.ronomepo-fg-success { color: @success; }
.ronomepo-info-bar {
  padding: 6px 10px;
  border-radius: 6px;
  background: alpha(@accent, 0.10);
  border: 1px solid alpha(@accent, 0.30);
}
.ronomepo-info-bar.ronomepo-tone-success { background: alpha(@success, 0.08); border-color: alpha(@success, 0.30); }
.ronomepo-info-bar.ronomepo-tone-warning { background: alpha(@warning, 0.08); border-color: alpha(@warning, 0.30); }
.ronomepo-info-bar.ronomepo-tone-danger { background: alpha(@danger, 0.08); border-color: alpha(@danger, 0.30); }
.ronomepo-stat { padding: 0 2px; }
.ronomepo-stat-tile {
  min-width: 84px;
  padding: 8px 12px;
  border-radius: 6px;
  border: 1px solid @border;
  background: alpha(@text_0, 0.03);
}
.ronomepo-stat-number { font-size: 18px; font-weight: 600; color: @text_0; }
.ronomepo-stat-number.ronomepo-fg-accent { color: @accent_strong; }
.ronomepo-stat-number.ronomepo-fg-warning { color: @warning; }
.ronomepo-stat-number.ronomepo-fg-danger { color: @danger; }
flowbox.ronomepo-wrap,
flowbox.ronomepo-wrap > flowboxchild { background: transparent; padding: 0; border: 0; }
.ronomepo-stat-value { color: @text_0; font-weight: 600; }
.ronomepo-stat-value.ronomepo-fg-danger { color: @danger; }
.ronomepo-stat-value.ronomepo-fg-success { color: @success; }
.ronomepo-stat-value.ronomepo-fg-warning { color: @warning; }
.ronomepo-stat-label { color: @text_2; font-size: 12px; }
.ronomepo-empty-state { padding: 32px 16px; }
.ronomepo-empty-state image { color: @text_2; -gtk-icon-size: 32px; }

/* Editors */
textview.ronomepo-editor,
textview.ronomepo-editor text { background: @workbench; color: @text_0; }

/* Console-style output */
textview.ronomepo-console,
textview.ronomepo-console text { background: @panel_bottom; color: @text_1; }

/* Popup menus */
popover.ronomepo-popup > contents { border-radius: 8px; padding: 4px; border: 1px solid @border_strong; }
.ronomepo-menu button {
  min-height: 26px;
  padding: 0 10px;
  border: 0;
  border-radius: 4px;
  background: transparent;
  box-shadow: none;
}
.ronomepo-menu button label { color: @text_0; font-weight: 400; }
.ronomepo-menu button:hover { background: alpha(@accent, 0.45); }
.ronomepo-menu-chevron { color: @text_2; }
"#;

thread_local! {
    static STYLESHEET_INSTALLED: Cell<bool> = const { Cell::new(false) };
}

/// Loads the plugin stylesheet once per process, on the GTK thread.
///
/// The host installs its theme at `STYLE_PROVIDER_PRIORITY_USER`, and GTK lets
/// a higher-priority provider win regardless of selector specificity, so a
/// provider at APPLICATION priority could not even color a label. The plugin
/// provider sits one step above the host; every selector is `ronomepo-`
/// scoped, so it only ever touches the plugin's own widgets.
pub fn install_stylesheet() {
    if STYLESHEET_INSTALLED.with(Cell::get) {
        return;
    }
    let Some(display) = gtk::gdk::Display::default() else {
        return;
    };
    let provider = gtk::CssProvider::new();
    provider.load_from_data(STYLESHEET);
    gtk::style_context_add_provider_for_display(
        &display,
        &provider,
        gtk::STYLE_PROVIDER_PRIORITY_USER + 1,
    );
    STYLESHEET_INSTALLED.with(|installed| installed.set(true));
}

/// Semantic color of a piece of text, badge or icon.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Tone {
    Strong,
    Neutral,
    Muted,
    Accent,
    Success,
    Warning,
    Danger,
}

impl Tone {
    const ALL: [Tone; 7] = [
        Tone::Strong,
        Tone::Neutral,
        Tone::Muted,
        Tone::Accent,
        Tone::Success,
        Tone::Warning,
        Tone::Danger,
    ];

    fn name(self) -> &'static str {
        match self {
            Tone::Strong => "strong",
            Tone::Neutral => "neutral",
            Tone::Muted => "muted",
            Tone::Accent => "accent",
            Tone::Success => "success",
            Tone::Warning => "warning",
            Tone::Danger => "danger",
        }
    }

    fn fg_class(self) -> String {
        format!("ronomepo-fg-{}", self.name())
    }

    fn tone_class(self) -> String {
        format!("ronomepo-tone-{}", self.name())
    }
}

/// Gives `widget` the text color of `tone`, replacing any previous tone.
pub fn set_fg_tone(widget: &impl IsA<Widget>, tone: Tone) {
    for other in Tone::ALL {
        if other != tone {
            widget.remove_css_class(&other.fg_class());
        }
    }
    widget.add_css_class(&tone.fg_class());
}

/// A left-aligned label with the given host text role (`body`, `meta`, ...).
pub fn text(content: &str, role: &str) -> Label {
    let label = Label::new(Some(content));
    label.set_xalign(0.0);
    label.add_css_class(&text_css_class(role));
    label
}

/// Muted, wrapping supporting text.
pub fn hint(content: &str) -> Label {
    let label = text(content, "body");
    label.set_wrap(true);
    label.add_css_class("ronomepo-fg-muted");
    label
}

/// A single-line label that ellipsizes and shows its full text on hover.
pub fn ellipsized(content: &str, classes: &[&str]) -> Label {
    let label = Label::new(Some(content));
    label.set_xalign(0.0);
    label.set_ellipsize(EllipsizeMode::End);
    label.set_tooltip_text(Some(content));
    for class in classes {
        label.add_css_class(class);
    }
    label
}

pub fn badge(content: &str, tone: Tone) -> Label {
    let label = Label::new(Some(content));
    label.set_valign(Align::Center);
    label.add_css_class("ronomepo-badge");
    label.add_css_class(&tone.tone_class());
    label
}

pub fn spacer() -> GtkBox {
    let spacer = GtkBox::new(Orientation::Horizontal, 0);
    spacer.set_hexpand(true);
    spacer
}

pub fn hbox(spacing: i32) -> GtkBox {
    GtkBox::new(Orientation::Horizontal, spacing)
}

pub fn vbox(spacing: i32) -> GtkBox {
    GtkBox::new(Orientation::Vertical, spacing)
}

/// Button styles: the host's semantic buttons plus a link-style action. Use
/// only one `Primary` per area, like IDEA's default button.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ButtonKind {
    Primary,
    Secondary,
    Ghost,
    Danger,
    Link,
}

pub fn button(label: &str, kind: ButtonKind) -> Button {
    let host_style = match kind {
        ButtonKind::Primary => "primary",
        ButtonKind::Secondary => "secondary",
        ButtonKind::Ghost => "ghost",
        ButtonKind::Danger => "danger",
        ButtonKind::Link => return link_button(label),
    };
    let button = Button::with_label(label);
    mark_clickable(&button);
    button.add_css_class(&button_css_class(host_style));
    button.set_valign(Align::Center);
    button
}

/// A link-style text action.
pub fn link_button(label: &str) -> Button {
    let button = Button::with_label(label);
    mark_clickable(&button);
    button.add_css_class("ronomepo-link");
    button.set_valign(Align::Center);
    button
}

pub fn icon_button(icon_name: &str, tooltip: &str) -> Button {
    let button = Button::new();
    style_icon_button(&button, icon_name, tooltip);
    button
}

pub fn icon_toggle(icon_name: &str, tooltip: &str) -> ToggleButton {
    let button = ToggleButton::new();
    style_icon_button(button.upcast_ref(), icon_name, tooltip);
    button
}

/// Turns any button (also a host-made toolbar button) into a flat 16px icon
/// button.
pub fn style_icon_button(button: &Button, icon_name: &str, tooltip: &str) {
    mark_clickable(button);
    button.add_css_class("ronomepo-icon-button");
    button.set_focus_on_click(false);
    button.set_valign(Align::Center);
    button.set_tooltip_text(Some(tooltip));
    button.set_child(Some(&Image::from_icon_name(icon_name)));
}

/// One underline tab of a tab bar; group them with `ToggleButton::set_group`.
pub fn tab_button(label: &str) -> ToggleButton {
    let button = ToggleButton::with_label(label);
    mark_clickable(&button);
    button.add_css_class("ronomepo-tab");
    button.set_focus_on_click(false);
    button
}

/// One single-choice filter chip; group them with `ToggleButton::set_group`.
pub fn chip_toggle(label: &str) -> ToggleButton {
    let button = ToggleButton::with_label(label);
    mark_clickable(&button);
    button.add_css_class("ronomepo-chip");
    button.set_focus_on_click(false);
    button.set_valign(Align::Center);
    button
}

/// A section of a settings-like page: a bold title followed by `extras`
/// (badges, or a spacer and actions), then the content.
pub fn section(title: &str, extras: &[&Widget], content: &impl IsA<Widget>) -> GtkBox {
    let block = vbox(8);
    let header = hbox(8);
    let heading = Label::new(Some(title));
    heading.set_xalign(0.0);
    heading.add_css_class("ronomepo-section-title");
    header.append(&heading);
    for extra in extras {
        header.append(*extra);
    }
    block.append(&header);
    block.append(content);
    block
}

/// Aligned `key  value` rows: muted keys, selectable wrapping values.
pub fn key_value_grid(rows: &[(&str, String)]) -> Grid {
    let grid = Grid::new();
    grid.set_column_spacing(16);
    grid.set_row_spacing(6);
    for (index, (key, value)) in rows.iter().enumerate() {
        let key_label = Label::new(Some(key));
        key_label.set_xalign(0.0);
        key_label.set_yalign(0.0);
        key_label.add_css_class("ronomepo-kv-key");
        let value_label = Label::new(Some(value));
        value_label.set_xalign(0.0);
        value_label.set_hexpand(true);
        value_label.set_wrap(true);
        value_label.set_wrap_mode(gtk::pango::WrapMode::WordChar);
        value_label.set_selectable(true);
        value_label.add_css_class("ronomepo-kv-value");
        grid.attach(&key_label, 0, index as i32, 1, 1);
        grid.attach(&value_label, 1, index as i32, 1, 1);
    }
    grid
}

pub fn card(child: &impl IsA<Widget>) -> GtkBox {
    let card = vbox(0);
    card.add_css_class("ronomepo-card");
    card.append(child);
    card
}

/// A bordered list whose items are separated by hairlines.
pub fn list_card() -> GtkBox {
    let list = vbox(0);
    list.add_css_class("ronomepo-list-card");
    list
}

pub fn list_card_item(list: &GtkBox, child: &impl IsA<Widget>) {
    child.add_css_class("ronomepo-list-item");
    list.append(child);
}

/// Severity of a Problems-style entry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Severity {
    Error,
    Warning,
    WeakWarning,
    Info,
}

impl Severity {
    fn icon(self) -> (&'static str, Tone) {
        match self {
            Severity::Error => ("dialog-error-symbolic", Tone::Danger),
            Severity::Warning => ("dialog-warning-symbolic", Tone::Warning),
            Severity::WeakWarning => ("dialog-warning-symbolic", Tone::Muted),
            Severity::Info => ("dialog-information-symbolic", Tone::Muted),
        }
    }
}

pub fn severity_icon(severity: Severity) -> Image {
    let (icon_name, tone) = severity.icon();
    let icon = Image::from_icon_name(icon_name);
    icon.set_valign(Align::Start);
    icon.set_margin_top(1);
    icon.add_css_class(&tone.fg_class());
    icon
}

/// An IDEA Problems entry: severity icon, message, muted location/metadata.
pub fn problem_row(severity: Severity, message: &str, meta: &str) -> GtkBox {
    let row = hbox(8);
    row.add_css_class("ronomepo-problem");
    row.append(&severity_icon(severity));
    let body = vbox(2);
    body.set_hexpand(true);
    let message = Label::new(Some(message));
    message.set_xalign(0.0);
    message.set_wrap(true);
    message.add_css_class("ronomepo-fg-strong");
    body.append(&message);
    if !meta.is_empty() {
        let meta = Label::new(Some(meta));
        meta.set_xalign(0.0);
        meta.set_wrap(true);
        meta.add_css_class("ronomepo-fg-muted");
        meta.add_css_class("ronomepo-meta");
        body.append(&meta);
    }
    row.append(&body);
    row
}

/// A tinted one-line status bar around `label`.
pub fn info_bar(label: &Label, icon_name: &str) -> GtkBox {
    let bar = hbox(8);
    bar.add_css_class("ronomepo-info-bar");
    let icon = Image::from_icon_name(icon_name);
    icon.add_css_class("ronomepo-fg-accent");
    bar.append(&icon);
    label.set_xalign(0.0);
    label.set_hexpand(true);
    label.set_wrap(true);
    bar.append(label);
    bar
}

/// Retints an info bar built by [`info_bar`].
pub fn set_info_bar_tone(bar: &GtkBox, tone: Tone) {
    for other in Tone::ALL {
        bar.remove_css_class(&other.tone_class());
    }
    bar.add_css_class(&tone.tone_class());
    if let Some(icon) = bar.first_child() {
        set_fg_tone(
            &icon,
            if tone == Tone::Neutral {
                Tone::Accent
            } else {
                tone
            },
        );
    }
}

/// A `value label` pair, e.g. "12 dirty", whose value can be updated.
pub fn stat(label: &str) -> (GtkBox, Label) {
    let stat = hbox(4);
    stat.add_css_class("ronomepo-stat");
    let value = Label::new(Some("0"));
    value.add_css_class("ronomepo-stat-value");
    let caption = Label::new(Some(label));
    caption.add_css_class("ronomepo-stat-label");
    stat.append(&value);
    stat.append(&caption);
    (stat, value)
}

/// A tile with a large number over a caption, for dashboard counters.
pub fn stat_tile(caption: &str, value: usize, tone_when_nonzero: Tone) -> GtkBox {
    let tile = vbox(2);
    tile.add_css_class("ronomepo-stat-tile");
    let number = Label::new(Some(&value.to_string()));
    number.set_xalign(0.0);
    number.add_css_class("ronomepo-stat-number");
    if value > 0 {
        number.add_css_class(&tone_when_nonzero.fg_class());
    }
    let caption = Label::new(Some(caption));
    caption.set_xalign(0.0);
    caption.add_css_class("ronomepo-stat-label");
    tile.append(&number);
    tile.append(&caption);
    tile
}

/// Lays `children` out left to right, wrapping onto new lines when narrow.
pub fn wrap_box(children: &[&Widget], spacing: u32) -> gtk::FlowBox {
    let flow = gtk::FlowBox::new();
    flow.add_css_class("ronomepo-wrap");
    flow.set_selection_mode(gtk::SelectionMode::None);
    flow.set_homogeneous(false);
    flow.set_column_spacing(spacing);
    flow.set_row_spacing(spacing);
    flow.set_max_children_per_line(64);
    flow.set_valign(Align::Start);
    for child in children {
        flow.insert(*child, -1);
    }
    let mut child = flow.first_child();
    while let Some(widget) = child {
        widget.set_focusable(false);
        child = widget.next_sibling();
    }
    flow
}

/// A centered icon, title, explanation and optional action.
pub fn empty_state(icon_name: &str, title: &str, body: &str, action: Option<&Button>) -> GtkBox {
    let empty = vbox(8);
    empty.add_css_class("ronomepo-empty-state");
    empty.set_halign(Align::Center);
    empty.set_valign(Align::Center);
    empty.append(&Image::from_icon_name(icon_name));
    let title = Label::new(Some(title));
    title.add_css_class("ronomepo-section-title");
    title.set_wrap(true);
    title.set_justify(gtk::Justification::Center);
    empty.append(&title);
    if !body.is_empty() {
        let body = Label::new(Some(body));
        body.add_css_class("ronomepo-fg-muted");
        body.set_wrap(true);
        body.set_max_width_chars(56);
        body.set_justify(gtk::Justification::Center);
        empty.append(&body);
    }
    if let Some(action) = action {
        action.set_halign(Align::Center);
        action.set_margin_top(4);
        empty.append(action);
    }
    empty
}

/// The title row of a page in the editor area.
pub fn page_title(title: &str) -> Label {
    let label = Label::new(Some(title));
    label.set_xalign(0.0);
    label.set_ellipsize(EllipsizeMode::End);
    label.add_css_class("ronomepo-page-title");
    label
}
