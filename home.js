'use strict';

const translations = {
  ar: {
    announcement: 'عروض مختارة وكوبونات مفيدة للجزائر',
    algeria: 'الجزائر',
    language: 'اللغة',
    navDeals: 'العروض',
    navDiscover: 'اكتشف',
    navHow: 'كيف يعمل',
    eyebrow: 'عروض ذكية • الجزائر',
    hero1: 'ابحث عن العرض',
    hero2: 'الذي يستحق الشراء',
    intro: 'نختار عروضاً وكوبونات ومنتجات رائجة، ثم نشاركها معك بطريقة واضحة وسريعة.',
    browse: 'تصفح العروض',
    choose: 'اختر بلدك',
    note: 'العروض المتاحة حالياً موجهة للجزائر.',
    benefit1: 'عروض منتقاة',
    benefit2: 'كوبونات مفيدة',
    benefit3: 'منتجات رائجة',
    benefit4: 'تحديثات مستمرة',
    discoverEyebrow: 'اكتشف DiscountWizard',
    discoverTitle: 'كل ما تحتاجه قبل اتخاذ قرار الشراء',
    discoverAside: 'نركز على العروض الواضحة، الكوبونات المفيدة والمنتجات التي تستحق المتابعة.',
    card1Title: 'عروض اليوم',
    card1Text: 'اكتشف تخفيضات ومنتجات مختارة من القنوات التي نتابعها.',
    card1Link: 'شاهد العروض',
    card2Title: 'كوبونات',
    card2Text: 'تابع الكوبونات والرموز المتاحة قبل إتمام عملية الشراء.',
    card2Link: 'استكشف الكوبونات',
    card3Title: 'آخر الصيحات',
    card3Text: 'منتجات منتشرة، أفكار مفيدة واكتشافات جديدة.',
    card3Link: 'اكتشف الجديد',
    available: 'متاح الآن',
    marketEyebrow: 'السوق الحالي',
    marketTitle: 'عروض مخصصة للجزائر',
    marketText: 'نشارك حالياً العروض عبر قناتنا. عند إضافة دول جديدة ستظهر هنا صفحات مستقلة لكل بلد.',
    chip1: 'عروض',
    chip2: 'كوبونات',
    telegram: 'انتقل إلى تيليغرام',
    marketNote: 'لا نعرض أسعاراً أو كوبونات منتهية الصلاحية هنا. تحقق من العرض قبل الشراء.',
    howEyebrow: 'كيف يعمل الموقع؟',
    howTitle: 'ثلاث خطوات بسيطة',
    step1Title: 'نلتقط العرض',
    step1Text: 'نراجع العروض والمنتجات التي تصل من القنوات والمصادر المختلفة.',
    step2Title: 'ننظم المعلومات',
    step2Text: 'نرتب العنوان والتفاصيل والكوبون والرابط لتكون واضحة.',
    step3Title: 'تصل إليك بسرعة',
    step3Text: 'ننشر العرض في الصفحة المناسبة حسب البلد واللغة.',
    joinEyebrow: 'لا تفوت العروض',
    joinTitle: 'تابع عروض الجزائر الآن',
    joinText: 'ستجد العروض الحالية عبر قناة تيليغرام، بينما نجهز صفحات مستقلة لبلدان أخرى.',
    joinButton: 'فتح قناة العروض',
    footerLine: 'عروض وكوبونات ومنتجات رائجة، منظمة حسب البلد واللغة.',
    disclosure: 'قد تحتوي بعض الروابط على روابط تسويقية.',
    marketDialogText: 'اختر السوق الذي تريد متابعة عروضه.',
    availableNow: 'متاح الآن',
    france: 'فرنسا',
    uk: 'المملكة المتحدة',
    spain: 'إسبانيا',
    russia: 'روسيا',
    korea: 'كوريا الجنوبية',
    soon: 'قريباً'
  },

  en: {
    announcement: 'Selected deals and useful coupons for Algeria',
    algeria: 'Algeria',
    language: 'Language',
    navDeals: 'Deals',
    navDiscover: 'Discover',
    navHow: 'How it works',
    eyebrow: 'Smart deals • Algeria',
    hero1: 'Find the deal',
    hero2: 'worth buying',
    intro: 'We select deals, coupons and trending products, then share them in a clear and simple way.',
    browse: 'Browse deals',
    choose: 'Choose your country',
    note: 'Current deals are intended for Algeria.',
    benefit1: 'Selected deals',
    benefit2: 'Useful coupons',
    benefit3: 'Trending products',
    benefit4: 'Regular updates',
    discoverEyebrow: 'Discover DiscountWizard',
    discoverTitle: 'Everything you need before deciding to buy',
    discoverAside: 'We focus on clear deals, useful coupons and products worth watching.',
    card1Title: 'Today’s deals',
    card1Text: 'Discover selected discounts and products from the channels we follow.',
    card1Link: 'View deals',
    card2Title: 'Coupons',
    card2Text: 'Follow available coupons and codes before completing your purchase.',
    card2Link: 'Explore coupons',
    card3Title: 'What’s trending',
    card3Text: 'Popular products, useful ideas and new discoveries.',
    card3Link: 'Discover more',
    available: 'Available now',
    marketEyebrow: 'Current market',
    marketTitle: 'Deals tailored for Algeria',
    marketText: 'We currently share deals through our channel. Dedicated pages for each country will appear as new markets are added.',
    chip1: 'Deals',
    chip2: 'Coupons',
    telegram: 'Open Telegram',
    marketNote: 'We do not list expired prices or coupons here. Check each offer before purchasing.',
    howEyebrow: 'How does it work?',
    howTitle: 'Three simple steps',
    step1Title: 'We capture the deal',
    step1Text: 'We review deals and products received from different channels and sources.',
    step2Title: 'We organize the details',
    step2Text: 'We format the title, details, coupon and link so everything is clear.',
    step3Title: 'It reaches you quickly',
    step3Text: 'We publish each deal on the right page for its country and language.',
    joinEyebrow: 'Do not miss a deal',
    joinTitle: 'Follow Algeria deals now',
    joinText: 'Current deals are available on our Telegram channel while we prepare dedicated country pages.',
    joinButton: 'Open deals channel',
    footerLine: 'Deals, coupons and trending products organized by country and language.',
    disclosure: 'Some links may be affiliate links.',
    marketDialogText: 'Choose the market whose deals you want to follow.',
    availableNow: 'Available now',
    france: 'France',
    uk: 'United Kingdom',
    spain: 'Spain',
    russia: 'Russia',
    korea: 'South Korea',
    soon: 'Coming soon'
  },

  fr: {
    announcement: 'Offres sélectionnées et coupons utiles pour l’Algérie',
    algeria: 'Algérie',
    language: 'Langue',
    navDeals: 'Offres',
    navDiscover: 'Découvrir',
    navHow: 'Comment ça marche',
    eyebrow: 'Bons plans intelligents • Algérie',
    hero1: 'Trouvez l’offre',
    hero2: 'qui mérite votre achat',
    intro: 'Nous sélectionnons des offres, des coupons et des produits tendance, puis nous les partageons clairement.',
    browse: 'Voir les offres',
    choose: 'Choisir votre pays',
    note: 'Les offres actuelles sont destinées à l’Algérie.',
    benefit1: 'Offres sélectionnées',
    benefit2: 'Coupons utiles',
    benefit3: 'Produits tendance',
    benefit4: 'Mises à jour régulières',
    discoverEyebrow: 'Découvrir DiscountWizard',
    discoverTitle: 'Tout ce dont vous avez besoin avant d’acheter',
    discoverAside: 'Nous privilégions les offres claires, les coupons utiles et les produits intéressants.',
    card1Title: 'Offres du jour',
    card1Text: 'Découvrez des réductions et produits sélectionnés depuis les chaînes que nous suivons.',
    card1Link: 'Voir les offres',
    card2Title: 'Coupons',
    card2Text: 'Consultez les coupons et codes disponibles avant de finaliser votre achat.',
    card2Link: 'Explorer les coupons',
    card3Title: 'Tendances',
    card3Text: 'Produits populaires, idées utiles et nouvelles découvertes.',
    card3Link: 'Découvrir',
    available: 'Disponible maintenant',
    marketEyebrow: 'Marché actuel',
    marketTitle: 'Offres pensées pour l’Algérie',
    marketText: 'Nous partageons actuellement les offres via notre chaîne. Des pages dédiées apparaîtront pour chaque pays.',
    chip1: 'Offres',
    chip2: 'Coupons',
    telegram: 'Ouvrir Telegram',
    marketNote: 'Vérifiez toujours l’offre avant l’achat.',
    howEyebrow: 'Comment ça marche ?',
    howTitle: 'Trois étapes simples',
    step1Title: 'Nous trouvons l’offre',
    step1Text: 'Nous examinons les offres et produits reçus depuis différentes sources.',
    step2Title: 'Nous organisons les détails',
    step2Text: 'Nous préparons le titre, les détails, le coupon et le lien.',
    step3Title: 'Elle arrive rapidement',
    step3Text: 'Chaque offre est publiée selon son pays et sa langue.',
    joinEyebrow: 'Ne manquez aucune offre',
    joinTitle: 'Suivez les offres Algérie',
    joinText: 'Les offres actuelles sont disponibles sur notre chaîne Telegram.',
    joinButton: 'Ouvrir la chaîne',
    footerLine: 'Offres, coupons et produits tendance organisés par pays et langue.',
    disclosure: 'Certains liens peuvent être affiliés.',
    marketDialogText: 'Choisissez le marché que vous souhaitez suivre.',
    availableNow: 'Disponible maintenant',
    france: 'France',
    uk: 'Royaume-Uni',
    spain: 'Espagne',
    russia: 'Russie',
    korea: 'Corée du Sud',
    soon: 'Bientôt disponible'
  },

  es: {
    announcement: 'Ofertas seleccionadas y cupones útiles para Argelia',
    algeria: 'Argelia',
    language: 'Idioma',
    navDeals: 'Ofertas',
    navDiscover: 'Descubrir',
    navHow: 'Cómo funciona',
    eyebrow: 'Ofertas inteligentes • Argelia',
    hero1: 'Encuentra la oferta',
    hero2: 'que merece la compra',
    intro: 'Seleccionamos ofertas, cupones y productos en tendencia para compartirlos de forma clara.',
    browse: 'Ver ofertas',
    choose: 'Elegir país',
    note: 'Las ofertas actuales están destinadas a Argelia.',
    benefit1: 'Ofertas seleccionadas',
    benefit2: 'Cupones útiles',
    benefit3: 'Productos en tendencia',
    benefit4: 'Actualizaciones frecuentes',
    discoverEyebrow: 'Descubre DiscountWizard',
    discoverTitle: 'Todo lo necesario antes de comprar',
    discoverAside: 'Nos centramos en ofertas claras, cupones útiles y productos interesantes.',
    card1Title: 'Ofertas de hoy',
    card1Text: 'Descubre descuentos y productos seleccionados de los canales que seguimos.',
    card1Link: 'Ver ofertas',
    card2Title: 'Cupones',
    card2Text: 'Consulta cupones y códigos disponibles antes de comprar.',
    card2Link: 'Explorar cupones',
    card3Title: 'Tendencias',
    card3Text: 'Productos populares, ideas útiles y nuevos descubrimientos.',
    card3Link: 'Descubrir más',
    available: 'Disponible ahora',
    marketEyebrow: 'Mercado actual',
    marketTitle: 'Ofertas para Argelia',
    marketText: 'Actualmente compartimos ofertas en nuestro canal. Se añadirán páginas para cada país.',
    chip1: 'Ofertas',
    chip2: 'Cupones',
    telegram: 'Abrir Telegram',
    marketNote: 'Comprueba cada oferta antes de comprar.',
    howEyebrow: '¿Cómo funciona?',
    howTitle: 'Tres pasos sencillos',
    step1Title: 'Encontramos la oferta',
    step1Text: 'Revisamos las ofertas y productos recibidos de varias fuentes.',
    step2Title: 'Organizamos la información',
    step2Text: 'Preparamos el título, los detalles, el cupón y el enlace.',
    step3Title: 'Llega rápidamente',
    step3Text: 'Publicamos cada oferta según su país e idioma.',
    joinEyebrow: 'No te pierdas ofertas',
    joinTitle: 'Sigue las ofertas de Argelia',
    joinText: 'Las ofertas actuales están disponibles en nuestro canal de Telegram.',
    joinButton: 'Abrir canal',
    footerLine: 'Ofertas, cupones y productos en tendencia por país e idioma.',
    disclosure: 'Algunos enlaces pueden ser de afiliados.',
    marketDialogText: 'Elige el mercado cuyas ofertas quieres seguir.',
    availableNow: 'Disponible ahora',
    france: 'Francia',
    uk: 'Reino Unido',
    spain: 'España',
    russia: 'Rusia',
    korea: 'Corea del Sur',
    soon: 'Próximamente'
  },

  ru: {
    announcement: 'Отобранные предложения и полезные купоны для Алжира',
    algeria: 'Алжир',
    language: 'Язык',
    navDeals: 'Предложения',
    navDiscover: 'Обзор',
    navHow: 'Как это работает',
    eyebrow: 'Умные предложения • Алжир',
    hero1: 'Найдите предложение',
    hero2: 'которое стоит купить',
    intro: 'Мы отбираем выгодные предложения, купоны и популярные товары, чтобы делиться ими понятно и быстро.',
    browse: 'Смотреть предложения',
    choose: 'Выбрать страну',
    note: 'Текущие предложения предназначены для Алжира.',
    benefit1: 'Отобранные предложения',
    benefit2: 'Полезные купоны',
    benefit3: 'Популярные товары',
    benefit4: 'Регулярные обновления',
    discoverEyebrow: 'О DiscountWizard',
    discoverTitle: 'Всё необходимое перед покупкой',
    discoverAside: 'Мы уделяем внимание понятным предложениям, полезным купонам и интересным товарам.',
    card1Title: 'Предложения дня',
    card1Text: 'Смотрите выбранные скидки и товары из каналов, за которыми мы следим.',
    card1Link: 'Смотреть предложения',
    card2Title: 'Купоны',
    card2Text: 'Проверьте доступные купоны и коды перед покупкой.',
    card2Link: 'Смотреть купоны',
    card3Title: 'Новинки',
    card3Text: 'Популярные товары, полезные идеи и новые находки.',
    card3Link: 'Узнать больше',
    available: 'Доступно сейчас',
    marketEyebrow: 'Текущий рынок',
    marketTitle: 'Предложения для Алжира',
    marketText: 'Сейчас мы публикуем предложения через наш канал. Страницы для других стран появятся позже.',
    chip1: 'Предложения',
    chip2: 'Купоны',
    telegram: 'Открыть Telegram',
    marketNote: 'Проверяйте каждое предложение перед покупкой.',
    howEyebrow: 'Как это работает?',
    howTitle: 'Три простых шага',
    step1Title: 'Мы находим предложение',
    step1Text: 'Мы проверяем предложения и товары из разных источников.',
    step2Title: 'Мы организуем детали',
    step2Text: 'Мы оформляем заголовок, детали, купон и ссылку.',
    step3Title: 'Оно быстро появляется',
    step3Text: 'Мы публикуем предложение для нужной страны и языка.',
    joinEyebrow: 'Не пропускайте предложения',
    joinTitle: 'Следите за предложениями Алжира',
    joinText: 'Текущие предложения доступны в нашем Telegram-канале.',
    joinButton: 'Открыть канал',
    footerLine: 'Предложения, купоны и популярные товары по странам и языкам.',
    disclosure: 'Некоторые ссылки могут быть партнёрскими.',
    marketDialogText: 'Выберите рынок, предложения которого хотите отслеживать.',
    availableNow: 'Доступно сейчас',
    france: 'Франция',
    uk: 'Великобритания',
    spain: 'Испания',
    russia: 'Россия',
    korea: 'Южная Корея',
    soon: 'Скоро'
  },

  ko: {
    announcement: '알제리를 위한 엄선된 할인과 유용한 쿠폰',
    algeria: '알제리',
    language: '언어',
    navDeals: '할인',
    navDiscover: '둘러보기',
    navHow: '이용 방법',
    eyebrow: '스마트 할인 • 알제리',
    hero1: '구매할 가치가 있는',
    hero2: '할인을 찾아보세요',
    intro: '할인, 쿠폰, 인기 상품을 선별하여 쉽고 명확하게 공유합니다.',
    browse: '할인 보기',
    choose: '국가 선택',
    note: '현재 할인은 알제리용입니다.',
    benefit1: '엄선된 할인',
    benefit2: '유용한 쿠폰',
    benefit3: '인기 상품',
    benefit4: '정기 업데이트',
    discoverEyebrow: 'DiscountWizard 소개',
    discoverTitle: '구매 전 필요한 모든 정보',
    discoverAside: '명확한 할인, 유용한 쿠폰, 주목할 상품에 집중합니다.',
    card1Title: '오늘의 할인',
    card1Text: '저희가 팔로우하는 채널의 할인과 상품을 확인하세요.',
    card1Link: '할인 보기',
    card2Title: '쿠폰',
    card2Text: '구매 전에 사용 가능한 쿠폰과 코드를 확인하세요.',
    card2Link: '쿠폰 보기',
    card3Title: '인기 상품',
    card3Text: '인기 상품, 유용한 아이디어와 새로운 발견.',
    card3Link: '더 보기',
    available: '현재 이용 가능',
    marketEyebrow: '현재 시장',
    marketTitle: '알제리 맞춤 할인',
    marketText: '현재 할인은 채널을 통해 공유됩니다. 다른 국가 페이지는 곧 추가됩니다.',
    chip1: '할인',
    chip2: '쿠폰',
    telegram: '텔레그램 열기',
    marketNote: '구매 전 각 제안을 확인하세요.',
    howEyebrow: '어떻게 작동하나요?',
    howTitle: '간단한 세 단계',
    step1Title: '할인을 찾습니다',
    step1Text: '다양한 출처에서 받은 할인과 상품을 검토합니다.',
    step2Title: '정보를 정리합니다',
    step2Text: '제목, 상세 정보, 쿠폰, 링크를 보기 좋게 정리합니다.',
    step3Title: '빠르게 전달합니다',
    step3Text: '각 국가와 언어에 맞는 페이지에 게시합니다.',
    joinEyebrow: '할인을 놓치지 마세요',
    joinTitle: '알제리 할인 팔로우하기',
    joinText: '현재 할인은 텔레그램 채널에서 확인할 수 있습니다.',
    joinButton: '채널 열기',
    footerLine: '국가와 언어별로 정리된 할인, 쿠폰, 인기 상품.',
    disclosure: '일부 링크는 제휴 링크일 수 있습니다.',
    marketDialogText: '팔로우할 시장을 선택하세요.',
    availableNow: '현재 이용 가능',
    france: '프랑스',
    uk: '영국',
    spain: '스페인',
    russia: '러시아',
    korea: '대한민국',
    soon: '준비 중'
  }
};

const languageSelect = document.getElementById('language');
const marketDialog = document.getElementById('markets');

function setLanguage(language) {
  const selectedLanguage = translations[language] ? language : 'ar';
  const text = translations[selectedLanguage];

  document.documentElement.lang = selectedLanguage;
  document.documentElement.dir = selectedLanguage === 'ar' ? 'rtl' : 'ltr';

  document.querySelectorAll('[data-t]').forEach((element) => {
    const key = element.dataset.t;
    if (text[key]) element.textContent = text[key];
  });

  if (languageSelect) languageSelect.value = selectedLanguage;

  document.title = `DiscountWizard — ${text.hero1} ${text.hero2}`;

  const description = document.querySelector('meta[name="description"]');
  if (description) description.content = text.intro;

  document.querySelectorAll('.brand').forEach((link) => {
    link.href = `?lang=${selectedLanguage}`;
  });

  const url = new URL(window.location.href);
  url.searchParams.set('lang', selectedLanguage);
  window.history.replaceState(null, '', url);

  try {
    localStorage.setItem('dw-language', selectedLanguage);
  } catch (_) {
    // تجاهل الخطأ إذا كان المتصفح يمنع التخزين المحلي
  }
}

let savedLanguage = 'ar';

try {
  savedLanguage = localStorage.getItem('dw-language') || 'ar';
} catch (_) {
  savedLanguage = 'ar';
}

const requestedLanguage = new URLSearchParams(window.location.search).get('lang');
setLanguage(requestedLanguage || savedLanguage);

if (languageSelect) {
  languageSelect.addEventListener('change', () => {
    setLanguage(languageSelect.value);
  });
}

document.querySelectorAll('.market-trigger').forEach((button) => {
  button.addEventListener('click', () => {
    if (marketDialog) marketDialog.showModal();
  });
});

const algeriaButton = document.getElementById('select-algeria');

if (algeriaButton) {
  algeriaButton.addEventListener('click', () => {
    if (marketDialog) marketDialog.close();
  });
}

if (marketDialog) {
  marketDialog.addEventListener('click', (event) => {
    if (event.target !== marketDialog) return;

    const dialogBox = marketDialog.getBoundingClientRect();

    const clickedOutside =
      event.clientX < dialogBox.left ||
      event.clientX > dialogBox.right ||
      event.clientY < dialogBox.top ||
      event.clientY > dialogBox.bottom;

    if (clickedOutside) marketDialog.close();
  });
}

const year = document.getElementById('year');

if (year) {
  year.textContent = new Date().getFullYear();
}