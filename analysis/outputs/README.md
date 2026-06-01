# Outputs de análise

Esta pasta recebe outputs pequenos gerados após a execução real do pipeline.

Arquivos esperados:

- `question_1_monthly_avg_total_amount/`: CSV pequeno com a resposta da pergunta obrigatória 1.
- `question_1_monthly_avg_total_amount.md`: versão Markdown da pergunta obrigatória 1.
- `question_2_may_hourly_avg_passengers/`: CSV pequeno com a resposta da pergunta obrigatória 2.
- `question_2_may_hourly_avg_passengers.md`: versão Markdown da pergunta obrigatória 2.
- `quality_report_summary/`: CSV pequeno com resumo do quality report, quando disponível.
- `quality_report_summary.md`: versão Markdown do resumo de qualidade.
- `execution_summary.md`: template para registrar a execução final.

Não salve Parquets grandes, dados de detalhe ou dumps completos nesta pasta.
