## Описание данных

Данные представляют собой сопоставленные предложения со статей таджикского и персидкого BBC. 

## Структура данных

Данные содержат следующие столбцы:

**tj**          -   Предложение на таджикском.
**pers**        -   Предложение на персидском.
**match**       -   Значение совпадение предложений от 0 до 1. Это значение является расстоянием Левенштейна, на предложениях из сопоставленных статей состоящих только из согласных.
**prob**        -   Значение совпадение статей от 0 до 1. Является косинусным расстоянием эмбидинга заголовков статьи.
**pair_id**     -   ID сопоставления статей.
**tj_id**       -   ID таджикской новости. Саму новость можно получить по ID с помощью **DumpReader** из **tajik_dump**
**tj_title**    -   Переведенный заголовок таджикской новости.
**tj_link**     -   Ссылка на таджикскую новость.
**fa_id**       -   ID персидской новости. Саму новость можно получить по ID с помощью **DumpReader** из **persian_dump**
**fa_title**    -   Переведенный заголовок персидской новости.
**fa_link**     -   Ссылка на персидскую новость.