(function () {
  function ensureBackToTopButton() {
    let button = document.querySelector("[data-back-to-top]");
    if (!(button instanceof HTMLElement)) {
      button = document.createElement("button");
      button.type = "button";
      button.className = "cards-back-to-top app-back-to-top";
      button.setAttribute("data-back-to-top", "true");
      button.setAttribute("aria-label", "Back to top");
      button.innerHTML = '<span aria-hidden="true">↑</span><span>Back to top</span>';
      document.body.appendChild(button);
    }
    return button;
  }

  function installBackToTop() {
    if (!(document.body instanceof HTMLElement)) return;
    const button = ensureBackToTopButton();
    if (!(button instanceof HTMLElement) || button.dataset.boundBackToTop === "true") return;
    button.dataset.boundBackToTop = "true";
    button.addEventListener("click", function () {
      window.scrollTo({ top: 0, behavior: "smooth" });
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", installBackToTop, { once: true });
  } else {
    installBackToTop();
  }
})();